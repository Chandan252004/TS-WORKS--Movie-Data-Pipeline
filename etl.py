#!/usr/bin/env python3
"""etl.py - MovieLens + OMDb movie data pipeline.

    Extract   movies.csv, ratings.csv (+ optional links.csv) and OMDb API details
    Transform clean titles/years/genres, validate ratings, parse API fields
    Load      upsert everything into SQLite using schema.sql (idempotent)
    Analyse   run queries.sql and print the answers


The file is organised in sections, top to bottom:
    1. Configuration      5. Enrichment (parallel lookups)
    2. Extract            6. Load (transactional upserts) + CSV export
    3. Transform          7. Analytical queries
    4. OMDb API client    8. Orchestration / command line

Running it again updates rows in place and never creates duplicates. OMDb
responses are cached, so repeated runs (e.g. once a day under the free-tier
limit of 1,000 calls) gradually enrich the whole catalogue.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

try:  # python-dotenv is optional; plain environment variables work without it
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

log = logging.getLogger("etl")

PROJECT_ROOT = Path(__file__).resolve().parent


# =============================================================================
# 1. CONFIGURATION: env vars / .env, overridden by command-line flags
# =============================================================================
@dataclass
class Config:
    data_dir: Path
    db_path: Path
    cache_path: Path
    export_path: Path
    omdb_api_key: str | None
    omdb_base_url: str = "https://www.omdbapi.com/"
    max_api_calls: int = 950          # free tier is 1,000/day; keep headroom
    request_timeout: float = 10.0     # seconds per HTTP request
    max_retries: int = 3              # retries on 5xx / network errors
    requests_per_second: float = 10.0 # shared throttle across all workers
    workers: int = 8                  # parallel API lookups (threads)
    offline: bool = False             # skip the API entirely (use cache only)

    @property
    def api_enabled(self) -> bool:
        return bool(self.omdb_api_key) and not self.offline


def load_config(**overrides) -> Config:
    """Build a Config from env vars, then apply any non-None overrides."""
    if load_dotenv is not None:
        load_dotenv(PROJECT_ROOT / ".env")

    cfg = Config(
        data_dir=Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data" / "raw")),
        db_path=Path(os.getenv("DB_PATH", PROJECT_ROOT / "data" / "movies.db")),
        cache_path=Path(os.getenv("OMDB_CACHE_PATH", PROJECT_ROOT / "data" / "cache" / "omdb_cache.sqlite")),
        export_path=Path(os.getenv("EXPORT_PATH", PROJECT_ROOT / "data" / "processed" / "movies_enriched.csv")),
        omdb_api_key=os.getenv("OMDB_API_KEY") or None,
        max_api_calls=int(os.getenv("OMDB_MAX_CALLS", 950)),
        workers=int(os.getenv("OMDB_WORKERS", 8)),
        requests_per_second=float(os.getenv("OMDB_RPS", 10)),
    )
    for key, value in overrides.items():
        if value is not None:
            setattr(cfg, key, Path(value) if key.endswith(("_dir", "_path")) else value)
    return cfg


# =============================================================================
# 2. EXTRACT: read and validate the MovieLens CSV files
# =============================================================================
# Explicit dtypes: fail fast on malformed files instead of silently guessing.
MOVIES_DTYPES = {"movieId": "int64", "title": "string", "genres": "string"}
RATINGS_DTYPES = {"userId": "int64", "movieId": "int64", "rating": "float64", "timestamp": "int64"}
LINKS_DTYPES = {"movieId": "int64", "imdbId": "string", "tmdbId": "string"}


def _require(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(
            f"Required input file not found: {path}. "
            "Download the MovieLens 'small' dataset and place the CSVs in the data directory."
        )
    return path


def _check_columns(df: pd.DataFrame, expected: dict, name: str) -> None:
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"{name} is missing expected columns: {sorted(missing)}")


def read_movies(data_dir: Path) -> pd.DataFrame:
    path = _require(data_dir / "movies.csv")
    df = pd.read_csv(path, dtype=MOVIES_DTYPES, encoding="utf-8")
    _check_columns(df, MOVIES_DTYPES, "movies.csv")
    log.info("Extracted %d movies from %s", len(df), path.name)
    return df


def read_ratings(data_dir: Path) -> pd.DataFrame:
    path = _require(data_dir / "ratings.csv")
    df = pd.read_csv(path, dtype=RATINGS_DTYPES, encoding="utf-8")
    _check_columns(df, RATINGS_DTYPES, "ratings.csv")
    log.info("Extracted %d ratings from %s", len(df), path.name)
    return df


def read_links(data_dir: Path) -> pd.DataFrame | None:
    """links.csv is optional. Returns None when it is not available."""
    path = data_dir / "links.csv"
    if not path.exists():
        log.info("links.csv not found; OMDb lookups will use title + year")
        return None
    # imdbId is read as a string to keep leading zeros ("0114709").
    df = pd.read_csv(path, dtype=LINKS_DTYPES, encoding="utf-8")
    _check_columns(df, LINKS_DTYPES, "links.csv")
    log.info("Extracted %d IMDb links from %s", len(df), path.name)
    return df

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"


def find_data_dir(preferred: Path) -> Path:
    """Use the configured folder, else the folder etl.py is in, else download MovieLens."""
    for candidate in (preferred, PROJECT_ROOT):
        if (candidate / "movies.csv").exists() and (candidate / "ratings.csv").exists():
            return candidate
    download_movielens(preferred)
    return preferred


def download_movielens(target: Path) -> None:
    """Fetch ml-latest-small.zip and keep movies.csv, ratings.csv and links.csv."""
    log.info("MovieLens CSVs not found; downloading %s", MOVIELENS_URL)
    target.mkdir(parents=True, exist_ok=True)
    resp = requests.get(MOVIELENS_URL, timeout=60)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for member in zf.namelist():
            name = Path(member).name
            if name in ("movies.csv", "ratings.csv", "links.csv"):
                (target / name).write_bytes(zf.read(member))
                log.info("Saved %s", target / name)


# =============================================================================
# 3. TRANSFORM: pure cleaning, parsing and feature-engineering functions
# =============================================================================
NO_GENRE = "(no genres listed)"

# "(1995)" or a series range "(2006–2007)" at the very end of the title.
_YEAR_RE = re.compile(r"\s*\((\d{4})(?:\s*[–-]\s*\d{4})?\)\s*$")
# A trailing parenthetical group such as "(a.k.a. Se7en)" or "(Léon)".
_TRAILING_PAREN_RE = re.compile(r"\s*\(([^()]*)\)\s*$")
# MovieLens moves leading articles to the end: "Matrix, The" / "Avventura, L'".
_ARTICLES = ("The", "A", "An", "Les", "La", "Le", "L'", "Il", "Das", "Der", "Die",
             "El", "Los", "Las", "Un", "Una", "Une", "Lo", "Det", "Den")
_ARTICLE_RE = re.compile(r"^(?P<body>.+), (?P<article>" + "|".join(re.escape(a) for a in _ARTICLES) + r")$")


# -----------------------------------------------------------------------------
# Title helpers
# -----------------------------------------------------------------------------
def _move_article_to_front(title: str) -> str:
    match = _ARTICLE_RE.match(title)
    if not match:
        return title
    article = match.group("article")
    sep = "" if article.endswith("'") else " "
    return f"{article}{sep}{match.group('body')}"


def parse_title(raw_title: str) -> dict:
    """Split a MovieLens title into its parts.

    >>> parse_title("Matrix, The (1999)")["clean_title"]
    'The Matrix'
    >>> parse_title("Seven (a.k.a. Se7en) (1995)")["alt_titles"]
    ['Se7en']
    """
    title = (raw_title or "").strip()

    year = None
    year_match = _YEAR_RE.search(title)
    if year_match:
        year = int(year_match.group(1))
        title = title[: year_match.start()]

    # Peel off trailing "(...)" groups: these are alternate / original-language titles.
    alt_titles: list[str] = []
    while True:
        paren = _TRAILING_PAREN_RE.search(title)
        if not paren or paren.start() == 0:
            break
        alt = re.sub(r"^a\.k\.a\.\s*", "", paren.group(1).strip(), flags=re.IGNORECASE)
        if alt:
            alt_titles.insert(0, _move_article_to_front(alt))
        title = title[: paren.start()]

    return {
        "clean_title": _move_article_to_front(title.strip()),
        "release_year": year,
        "alt_titles": alt_titles,
    }


def split_genres(genres: str | None) -> list[str]:
    """'Action|Sci-Fi' -> ['Action', 'Sci-Fi']; the 'no genres' marker -> []."""
    if genres is None or pd.isna(genres):
        return []
    return [g.strip() for g in str(genres).split("|") if g.strip() and g.strip() != NO_GENRE]


# -----------------------------------------------------------------------------
# Movies
# -----------------------------------------------------------------------------
def transform_movies(movies: pd.DataFrame, links: pd.DataFrame | None = None) -> pd.DataFrame:
    """Clean movies.csv: parse the release year out of the title and split the genres.

    clean_title / alt_titles are only used to search OMDb; they are not stored.
    """
    df = movies.copy()
    df["title"] = df["title"].str.strip()

    before = len(df)
    df = df.drop_duplicates(subset="movieId", keep="first")
    if len(df) != before:
        log.warning("Dropped %d duplicate movieId rows", before - len(df))

    parsed = df["title"].map(parse_title).apply(pd.Series)
    df = pd.concat([df, parsed], axis=1)
    df["release_year"] = df["release_year"].astype("Int64")
    df["genre_list"] = df["genres"].map(split_genres)

    if links is not None:
        imdb = links[["movieId", "imdbId"]].dropna(subset=["imdbId"]).copy()
        # links.csv stores "114709"; OMDb expects "tt0114709" (at least 7 digits).
        imdb["imdb_id"] = "tt" + imdb["imdbId"].str.strip().str.zfill(7)
        df = df.merge(imdb[["movieId", "imdb_id"]], on="movieId", how="left")
    else:
        df["imdb_id"] = None

    missing_year = int(df["release_year"].isna().sum())
    if missing_year:
        log.info("%d movies have no year in the title (OMDb will be used as fallback)", missing_year)

    return df.rename(columns={"movieId": "movie_id"})[
        ["movie_id", "title", "clean_title", "alt_titles", "release_year", "imdb_id", "genre_list"]
    ]


def build_movie_genres(movies: pd.DataFrame) -> pd.DataFrame:
    """Explode genre_list into one (movie_id, genre) row per genre."""
    exploded = movies[["movie_id", "genre_list"]].explode("genre_list").dropna()
    return exploded.rename(columns={"genre_list": "genre"}).drop_duplicates().reset_index(drop=True)


# -----------------------------------------------------------------------------
# Ratings
# -----------------------------------------------------------------------------
def transform_ratings(ratings: pd.DataFrame, valid_movie_ids) -> pd.DataFrame:
    """Validate ratings; the four columns of ratings.csv are kept as they are."""
    df = ratings.dropna(subset=["userId", "movieId", "rating", "timestamp"]).copy()

    # Ratings must be 0.5-5.0 in half-star steps.
    valid = df["rating"].between(0.5, 5.0) & ((df["rating"] * 2) % 1 == 0)
    if (~valid).any():
        log.warning("Dropped %d ratings outside 0.5-5.0 half-star scale", int((~valid).sum()))
    df = df[valid]

    # Referential integrity: a rating must point at a known movie.
    known = df["movieId"].isin(set(valid_movie_ids))
    if (~known).any():
        log.warning("Dropped %d ratings for unknown movieIds", int((~known).sum()))
    df = df[known]

    # Keep only the latest rating if a user rated the same movie twice.
    df = df.sort_values("timestamp").drop_duplicates(subset=["userId", "movieId"], keep="last")

    return df.rename(columns={"userId": "user_id", "movieId": "movie_id"})[
        ["user_id", "movie_id", "rating", "timestamp"]
    ].reset_index(drop=True)


# -----------------------------------------------------------------------------
# OMDb response parsing
# -----------------------------------------------------------------------------
def _na(value):
    """OMDb uses the literal string 'N/A' for missing values."""
    if value is None:
        return None
    value = str(value).strip()
    return None if value in ("", "N/A") else value


def _to_int(value, strip: str = "") -> int | None:
    value = _na(value)
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", value.replace(strip, "") if strip else value)
    return int(digits) if digits else None


def _to_float(value) -> float | None:
    value = _na(value)
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def _to_iso_date(value) -> str | None:
    value = _na(value)
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%d %b %Y").date().isoformat()
    except ValueError:
        return None


def parse_omdb_payload(payload: dict | None) -> dict:
    """Keep only the OMDb fields we store, converted to proper types."""
    if not payload or payload.get("Response") != "True":
        return {}
    year = _na(payload.get("Year"))
    runtime = _na(payload.get("Runtime"))
    return {
        "imdb_id": _na(payload.get("imdbID")),
        "omdb_year": int(year[:4]) if year and year[:4].isdigit() else None,
        "released_date": _to_iso_date(payload.get("Released")),
        "runtime_minutes": _to_int(runtime.split()[0]) if runtime else None,
        "language": _na(payload.get("Language")),
        "country": _na(payload.get("Country")),
        "imdb_rating": _to_float(payload.get("imdbRating")),
        "box_office_usd": _to_int(payload.get("BoxOffice")),
        "directors": split_people(payload.get("Director")),
    }


def split_people(value) -> list[str]:
    """'Lana Wachowski, Lilly Wachowski' -> ['Lana Wachowski', 'Lilly Wachowski'].

    OMDb sometimes annotates credits, e.g. 'Ridley Scott (director's cut)';
    the annotation is removed so the same person is counted once.
    """
    value = _na(value)
    if value is None:
        return []
    people = [re.sub(r"\s*\(.*?\)\s*", "", p).strip() for p in value.split(",")]
    seen, result = set(), []
    for person in people:
        if person and person not in seen:
            seen.add(person)
            result.append(person)
    return result


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# 4. OMDb API CLIENT: cache, retries, shared rate limit, daily budget, fallback lookups
# =============================================================================
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class OmdbAuthError(RuntimeError):
    """The API key is missing or invalid: no point continuing to call the API."""


class BudgetExhausted(RuntimeError):
    """The daily request limit (ours or OMDb's) has been reached."""


@dataclass
class LookupResult:
    status: str                      # found | not_found | error | skipped
    method: str | None = None        # imdb_id | title_year | title | alt_title
    payload: dict | None = None
    fetched_at: str | None = None


@dataclass
class ClientStats:
    api_calls: int = 0
    cache_hits: int = 0
    errors: int = 0
    by_status: dict = field(default_factory=dict)


class ResponseCache:
    """Tiny persistent key-value cache backed by its own SQLite file.

    Kept separate from the warehouse DB so dropping / rebuilding the warehouse
    never throws away API responses we already paid for.
    """

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # One connection shared by all worker threads, guarded by a lock.
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS omdb_cache ("
            " cache_key TEXT PRIMARY KEY, response_json TEXT NOT NULL, fetched_at TEXT NOT NULL)"
        )
        self.conn.commit()

    def get(self, key: str) -> tuple[dict, str] | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT response_json, fetched_at FROM omdb_cache WHERE cache_key = ?", (key,)
            ).fetchone()
        return (json.loads(row[0]), row[1]) if row else None

    def put(self, key: str, payload: dict, fetched_at: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO omdb_cache (cache_key, response_json, fetched_at) VALUES (?, ?, ?) "
                "ON CONFLICT(cache_key) DO UPDATE SET response_json = excluded.response_json, "
                "fetched_at = excluded.fetched_at",
                (key, json.dumps(payload), fetched_at),
            )
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class OmdbClient:
    def __init__(
        self,
        api_key: str | None,
        cache: ResponseCache,
        base_url: str = "https://www.omdbapi.com/",
        max_calls: int = 950,
        timeout: float = 10.0,
        max_retries: int = 3,
        requests_per_second: float = 5.0,
        session: requests.Session | None = None,
        sleep=time.sleep,
        plot: str = "short",
    ):
        self.api_key = api_key
        self.cache = cache
        self.base_url = base_url
        self.max_calls = max_calls
        self.timeout = timeout
        self.max_retries = max_retries
        self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self.session = session or requests.Session()
        self._sleep = sleep
        self.plot = plot                  # the plot is not stored, so the short one is enough
        self._next_slot = 0.0             # earliest time the next request may start
        self._lock = threading.Lock()     # guards _next_slot, budget and stats
        self.budget_exhausted = api_key is None
        self.stats = ClientStats()

    # ------------------------------------------------------------------ HTTP
    def _throttle(self) -> None:
        """Shared rate limiter: hands out evenly spaced start times to all threads."""
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self.min_interval
        if slot > now:
            self._sleep(slot - now)

    def _reserve_call(self) -> None:
        """Count a request against the budget before it is sent (atomic across threads)."""
        with self._lock:
            if self.budget_exhausted or self.stats.api_calls >= self.max_calls:
                self.budget_exhausted = True
                raise BudgetExhausted("API call budget for this run is used up")
            self.stats.api_calls += 1

    def _count(self, field_name: str, key: str | None = None) -> None:
        with self._lock:
            if key is None:
                setattr(self.stats, field_name, getattr(self.stats, field_name) + 1)
            else:
                self.stats.by_status[key] = self.stats.by_status.get(key, 0) + 1

    def _call_api(self, params: dict) -> dict:
        """One logical request, with exponential-backoff retries on transient failures."""
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._reserve_call()
            self._throttle()
            try:
                resp = self.session.get(
                    self.base_url, params={**params, "apikey": self.api_key}, timeout=self.timeout
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
            else:
                if resp.status_code == 401:
                    error = _safe_json(resp).get("Error", "")
                    if "limit" in error.lower():
                        self.budget_exhausted = True
                        raise BudgetExhausted(f"OMDb says: {error}")
                    raise OmdbAuthError(f"OMDb rejected the API key: {error or resp.text[:100]}")
                if resp.status_code in RETRYABLE_STATUS:
                    last_error = RuntimeError(f"HTTP {resp.status_code}")
                else:
                    resp.raise_for_status()
                    return _safe_json(resp)
            if attempt < self.max_retries:
                backoff = 2 ** attempt
                log.debug("OMDb request failed (%s); retrying in %ss", last_error, backoff)
                self._sleep(backoff)
        raise RuntimeError(f"OMDb request failed after {self.max_retries + 1} attempts: {last_error}")

    # ---------------------------------------------------------- cache + HTTP
    def _fetch(self, cache_key: str, params: dict) -> tuple[dict, str]:
        cached = self.cache.get(cache_key)
        if cached is not None:
            self._count("cache_hits")
            return cached
        payload = self._call_api(params)
        fetched_at = utc_now_iso()
        # Cache "not found" answers too: they are stable and would waste budget.
        if payload.get("Response") in ("True", "False"):
            self.cache.put(cache_key, payload, fetched_at)
        return payload, fetched_at

    # ------------------------------------------------------- lookup strategy
    def lookup(self, imdb_id: str | None, title: str, year: int | None,
               alt_titles: list[str] | None = None) -> LookupResult:
        """Try progressively looser lookups until one returns a movie."""
        attempts: list[tuple[str, str, dict, int | None]] = []
        if imdb_id:
            attempts.append(("imdb_id", f"i:{imdb_id}", {"i": imdb_id}, None))
        if year:
            attempts.append(("title_year", f"t:{title.lower()}|y:{year}", {"t": title, "y": year}, None))
            # Release years often differ by one between sources; accept +/-1 only.
            attempts.append(("title", f"t:{title.lower()}", {"t": title}, year))
        else:
            attempts.append(("title", f"t:{title.lower()}", {"t": title}, None))
        for alt in alt_titles or []:
            params = {"t": alt, "y": year} if year else {"t": alt}
            attempts.append(("alt_title", f"t:{alt.lower()}|y:{year or ''}", params, None))

        result = LookupResult(status="not_found")
        for method, key, params, verify_year in attempts:
            try:
                payload, fetched_at = self._fetch(key, {**params, "plot": self.plot, "r": "json"})
            except BudgetExhausted:
                result = LookupResult(status="skipped")
                break
            except OmdbAuthError:
                raise
            except Exception as exc:  # network / HTTP errors after retries
                log.warning("OMDb lookup failed for %r (%s): %s", title, method, exc)
                self._count("errors")
                result = LookupResult(status="error")
                continue

            if payload.get("Response") == "True" and _year_ok(payload, verify_year):
                result = LookupResult("found", method, payload, fetched_at)
                break

        self._count("by_status", result.status)
        return result


def _year_ok(payload: dict, expected: int | None) -> bool:
    if expected is None:
        return True
    year = str(payload.get("Year", ""))[:4]
    return year.isdigit() and abs(int(year) - expected) <= 1


def _safe_json(resp) -> dict:
    try:
        data = resp.json()
        return data if isinstance(data, dict) else {}
    except ValueError:
        return {}


# =============================================================================
# 5. ENRICHMENT: parallel OMDb lookups, most-rated movies first
# =============================================================================
OMDB_COLUMNS = ["released_date", "runtime_minutes", "language", "country", "imdb_rating", "box_office_usd"]


def enrich_movies(movies: pd.DataFrame, ratings: pd.DataFrame, client: OmdbClient,
                  workers: int = 1, progress_every: int = 1000
                  ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Look every movie up in OMDb and merge the results into the movies frame.

    Movies are processed most-rated first, so when the daily API budget runs
    out the movies that matter most to the analytics are already enriched.

    `workers` > 1 runs lookups in a thread pool. API calls are network-bound
    (the CPU mostly waits), so threads overlap the waiting; the client's shared
    rate limiter still caps requests per second, so OMDb sees a steady pace.

    Returns (movies with OMDb columns, movie_directors, lookup status counts).
    """
    results = lookup_all(movies, ratings, client, workers=workers, progress_every=progress_every)

    detail_rows, director_rows, status_counts = [], [], {}
    for row, result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
        parsed = parse_omdb_payload(result.payload) if result.status == "found" else {}
        detail_rows.append({"movie_id": row.movie_id, "omdb_imdb_id": parsed.get("imdb_id"),
                            "omdb_year": parsed.get("omdb_year"), **{c: parsed.get(c) for c in OMDB_COLUMNS}})
        for director in parsed.get("directors", []):
            director_rows.append({"movie_id": row.movie_id, "director": director})

    details = pd.DataFrame(detail_rows)
    movie_directors = pd.DataFrame(director_rows, columns=["movie_id", "director"]).drop_duplicates()

    # Fill gaps from OMDb: the IMDb id, and the year for titles that have none.
    enriched = movies.merge(details, on="movie_id", how="left")
    enriched["release_year"] = enriched["release_year"].fillna(enriched["omdb_year"]).astype("Int64")
    enriched["imdb_id"] = enriched["imdb_id"].where(enriched["imdb_id"].notna(), enriched["omdb_imdb_id"])
    return enriched.drop(columns=["omdb_imdb_id", "omdb_year"]), movie_directors, status_counts


def lookup_all(movies: pd.DataFrame, ratings: pd.DataFrame, client: OmdbClient,
               workers: int = 1, progress_every: int = 1000) -> list:
    """Look every movie up (most-rated first) and return [(movie_row, LookupResult), ...].

    Shared by the ETL and the Excel automation, so both get the same parallel,
    cached, budget-aware fetching.
    """
    popularity = ratings.groupby("movie_id").size()
    order = movies.assign(_n=movies["movie_id"].map(popularity).fillna(0)) \
                  .sort_values(["_n", "movie_id"], ascending=[False, True])

    rows = list(order.itertuples(index=False))
    done, done_lock = 0, threading.Lock()

    def lookup(row):
        nonlocal done
        year = None if pd.isna(row.release_year) else int(row.release_year)
        result = client.lookup(row.imdb_id if isinstance(row.imdb_id, str) else None,
                               row.clean_title, year, row.alt_titles)
        with done_lock:
            done += 1
            if done % progress_every == 0:
                log.info("OMDb progress: %d/%d movies (api calls=%d, cache hits=%d)",
                         done, len(rows), client.stats.api_calls, client.stats.cache_hits)
        return row, result

    if workers > 1:
        # map() keeps input order, so results line up with the most-rated-first order.
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="omdb") as pool:
            results = list(pool.map(lookup, rows))
    else:
        results = [lookup(row) for row in rows]
    return results


# =============================================================================
# 6. LOAD: transactional upserts into SQLite, plus a flat CSV export
# =============================================================================
SCHEMA_FILE = PROJECT_ROOT / "schema.sql"
QUERIES_FILE = PROJECT_ROOT / "queries.sql"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


class SchemaMismatch(RuntimeError):
    """The database file was created by an older version of schema.sql."""


def apply_schema(conn: sqlite3.Connection, schema_file: Path = SCHEMA_FILE) -> None:
    conn.executescript(schema_file.read_text(encoding="utf-8"))
    # CREATE TABLE IF NOT EXISTS leaves an existing table untouched, so check that
    # an existing database really has the columns this version of etl.py writes.
    expected = {"movies": ["movie_id", "title", "release_year", "imdb_id", *OMDB_COLUMNS],
                "ratings": ["user_id", "movie_id", "rating", "timestamp"]}
    for table, columns in expected.items():
        actual = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
        if actual != columns:
            raise SchemaMismatch(f"The database file was built with an older schema (table {table} has "
                                 f"columns {actual}). Replace the data folder with the one from the latest "
                                 "zip, or delete the database file to rebuild it.")
    log.info("Schema applied from %s", schema_file.name)


def _none_if_na(value):
    """pandas NA/NaN -> None so sqlite3 stores NULL."""
    if isinstance(value, (list, dict)):
        return value
    if pd.isna(value):
        return None
    # numpy scalars (int64, float64) -> native Python, otherwise sqlite3 stores BLOBs
    return value.item() if hasattr(value, "item") else value


def _rows(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(_none_if_na(v) for v in row) for row in df[columns].itertuples(index=False, name=None)]


# -----------------------------------------------------------------------------
def load_movies(conn: sqlite3.Connection, movies: pd.DataFrame) -> int:
    """Upsert movies. OMDb columns use COALESCE, so a run where a movie was not
    looked up (offline, or the daily limit was reached) never erases earlier OMDb data."""
    cols = ["movie_id", "title", "release_year", "imdb_id", *OMDB_COLUMNS]
    keep_old = ",\n            ".join(f"{c} = COALESCE(excluded.{c}, movies.{c})" for c in ["release_year", "imdb_id", *OMDB_COLUMNS])
    conn.executemany(
        f"""
        INSERT INTO movies ({", ".join(cols)}) VALUES ({", ".join("?" * len(cols))})
        ON CONFLICT(movie_id) DO UPDATE SET
            title        = excluded.title,
            {keep_old}
        """,
        _rows(movies, cols),
    )
    return len(movies)


def load_genres(conn: sqlite3.Connection, movie_genres: pd.DataFrame, movie_ids) -> int:
    """Refresh genre links for every loaded movie (also clears genres a movie lost)."""
    names = sorted(movie_genres["genre"].unique())
    conn.executemany("INSERT INTO genres (name) VALUES (?) ON CONFLICT(name) DO NOTHING", [(n,) for n in names])
    genre_ids = dict(conn.execute("SELECT name, genre_id FROM genres"))

    conn.executemany("DELETE FROM movie_genres WHERE movie_id = ?", [(int(m),) for m in movie_ids])
    conn.executemany(
        "INSERT INTO movie_genres (movie_id, genre_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
        [(int(m), genre_ids[g]) for m, g in movie_genres[["movie_id", "genre"]].itertuples(index=False)],
    )
    return len(movie_genres)


def load_directors(conn: sqlite3.Connection, movie_directors: pd.DataFrame) -> int:
    """movie_directors has columns movie_id, director (only for found movies)."""
    if movie_directors.empty:
        return 0
    names = sorted(movie_directors["director"].unique())
    conn.executemany("INSERT INTO directors (name) VALUES (?) ON CONFLICT(name) DO NOTHING", [(n,) for n in names])
    director_ids = dict(conn.execute("SELECT name, director_id FROM directors"))

    movie_ids = movie_directors["movie_id"].unique().tolist()
    conn.executemany("DELETE FROM movie_directors WHERE movie_id = ?", [(int(m),) for m in movie_ids])
    conn.executemany(
        "INSERT INTO movie_directors (movie_id, director_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
        [(int(m), director_ids[d]) for m, d in movie_directors[["movie_id", "director"]].itertuples(index=False)],
    )
    return len(movie_directors)


def load_ratings(conn: sqlite3.Connection, ratings: pd.DataFrame) -> int:
    conn.executemany(
        """
        INSERT INTO ratings (user_id, movie_id, rating, timestamp) VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, movie_id) DO UPDATE SET
            rating    = excluded.rating,
            timestamp = excluded.timestamp
        """,
        _rows(ratings, ["user_id", "movie_id", "rating", "timestamp"]),
    )
    return len(ratings)


# -----------------------------------------------------------------------------
# Export
# -----------------------------------------------------------------------------
ENRICHED_FILTER = "m.imdb_rating IS NOT NULL OR m.released_date IS NOT NULL OR m.runtime_minutes IS NOT NULL " \
                  "OR EXISTS (SELECT 1 FROM movie_directors md WHERE md.movie_id = m.movie_id)"

EXPORT_SQL = f"""
SELECT m.movie_id, m.title, m.release_year, m.imdb_id,
       (SELECT GROUP_CONCAT(g.name, '|') FROM movie_genres mg JOIN genres g USING (genre_id)
         WHERE mg.movie_id = m.movie_id)                         AS genres,
       (SELECT GROUP_CONCAT(d.name, ', ') FROM movie_directors md JOIN directors d USING (director_id)
         WHERE md.movie_id = m.movie_id)                         AS directors,
       m.released_date, m.runtime_minutes, m.language, m.country, m.imdb_rating, m.box_office_usd
FROM movies m
WHERE {ENRICHED_FILTER}
ORDER BY (SELECT COUNT(*) FROM ratings r WHERE r.movie_id = m.movie_id) DESC, m.movie_id
"""


def export_enriched_csv(conn: sqlite3.Connection, path: Path) -> int:
    """Write one flat row per OMDb-enriched movie (MovieLens + OMDb fields) to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_sql_query(EXPORT_SQL, conn)
    # Whole-number columns with NULLs come back as floats (1995.0); keep them integers.
    for col in ["release_year", "runtime_minutes", "box_office_usd"]:
        df[col] = df[col].astype("Int64")
    df.to_csv(path, index=False)
    return len(df)


# =============================================================================
# 7. ANALYTICAL QUERIES: run queries.sql and print the answers
# =============================================================================
def split_statements(sql_text: str) -> list[tuple[str, str]]:
    """Return (label, statement) pairs; the label is the '-- Qn.' comment above it."""
    statements = []
    for chunk in re.split(r";[ \t]*(?:\r?\n|$)", sql_text):
        lines = chunk.splitlines()
        sql = "\n".join(l for l in lines if l.strip() and not l.strip().startswith("--")).strip()
        if not sql:
            continue
        headings = [m.group(1).strip() for l in lines
                    if (m := re.match(r"\s*--\s*(Q\d+\..*)", l))]
        statements.append((headings[0] if headings else f"Query {len(statements) + 1}", sql))
    return statements


MODEL_TABLES = ["movies", "ratings", "genres", "movie_genres", "directors", "movie_directors"]


def print_pipeline_summary(cfg, metrics: dict) -> None:
    """Short, readable report of what each pipeline stage did and what was loaded."""
    conn = sqlite3.connect(cfg.db_path)
    count = lambda sql: conn.execute(sql).fetchone()[0]
    rows = {t: count(f"SELECT COUNT(*) FROM {t}") for t in MODEL_TABLES}
    found = count(f"SELECT COUNT(*) FROM movies m WHERE {ENRICHED_FILTER}")
    conn.close()
    print("\nPIPELINE")
    print(f"  1. Extract    {metrics['movies_extracted']:,} movies and {metrics['ratings_extracted']:,} ratings from MovieLens CSVs")
    print(f"  2. Transform  release year parsed from titles; {metrics['genre_pairs']:,} movie-genre pairs; "
          f"{metrics['ratings_dropped']:,} invalid ratings dropped")
    print(f"  3. Enrich     {found:,} movies have OMDb details (director, release date, box office, ...); "
          f"{metrics['api_calls']:,} API calls this run")
    print(f"  4. Load       SQLite database {cfg.db_path.name} in {metrics['seconds']:.1f}s; "
          f"{metrics['exported']:,} enriched movies exported to {cfg.export_path.name}")
    print("\nDATA MODEL (table: rows)")
    names = list(rows)
    for i in range(0, len(names), 3):
        print("  " + "".join(f"{n + ':':<17}{rows[n]:>9,}     " for n in names[i:i + 3]).rstrip())


def print_query_results(db_path: Path) -> None:
    """Run every statement in queries.sql and print only the question and its answer."""
    conn = sqlite3.connect(db_path)
    pd.set_option("display.width", 160)
    pd.set_option("display.max_colwidth", 60)
    for label, sql in split_statements(QUERIES_FILE.read_text(encoding="utf-8-sig")):
        df = pd.read_sql_query(sql, conn)
        print(f"\n{label}")
        if df.empty:
            print("(no rows yet: this question needs OMDb data)")
        else:
            print(df.to_string(index=False))
    print()
    conn.close()


# =============================================================================
# 8. ORCHESTRATION AND COMMAND LINE
# =============================================================================
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MovieLens + OMDb ETL pipeline")
    p.add_argument("--data-dir", help="folder containing movies.csv and ratings.csv")
    p.add_argument("--db-path", help="SQLite database file to load into")
    p.add_argument("--cache-path", help="SQLite file used to cache OMDb responses")
    p.add_argument("--max-api-calls", type=int, help="max OMDb requests this run (default 950)")
    p.add_argument("--workers", type=int, help="parallel OMDb lookups (default 8; 1 = sequential)")
    p.add_argument("--rps", type=float, help="max OMDb requests per second across all workers (default 10)")
    p.add_argument("--offline", action="store_true", default=None,
                   help="do not call the OMDb API; use cached responses only")
    p.add_argument("--run-queries", action="store_true",
                   help="kept for compatibility; the answers are always printed")
    p.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="default WARNING (only the results are printed); INFO shows every step")
    return p.parse_args(argv)


def run(cfg) -> dict:
    """Execute the full pipeline and return run metrics."""
    started = time.perf_counter()

    # ---------------------------------------------------------------- Extract
    cfg.data_dir = find_data_dir(cfg.data_dir)
    movies_raw = read_movies(cfg.data_dir)
    ratings_raw = read_ratings(cfg.data_dir)
    links_raw = read_links(cfg.data_dir)

    # -------------------------------------------------------------- Transform
    movies = transform_movies(movies_raw, links_raw)
    ratings = transform_ratings(ratings_raw, movies["movie_id"])
    movie_genres = build_movie_genres(movies)
    log.info("Transformed: %d movies, %d ratings, %d movie-genre pairs",
             len(movies), len(ratings), len(movie_genres))
    extract_stats = {"movies_extracted": len(movies_raw), "ratings_extracted": len(ratings_raw),
                     "ratings_dropped": len(ratings_raw) - len(ratings), "genre_pairs": len(movie_genres)}

    # ----------------------------------------------------------------- Enrich
    if not cfg.omdb_api_key and not cfg.offline:
        log.info("OMDB_API_KEY is not set: running from the response cache only")
    cache = ResponseCache(cfg.cache_path)
    client = OmdbClient(
        api_key=cfg.omdb_api_key if cfg.api_enabled else None,
        cache=cache,
        base_url=cfg.omdb_base_url,
        max_calls=cfg.max_api_calls,
        timeout=cfg.request_timeout,
        max_retries=cfg.max_retries,
        requests_per_second=cfg.requests_per_second,
    )
    try:
        enrich_started = time.perf_counter()
        movies, movie_directors, status_counts = enrich_movies(movies, ratings, client, workers=cfg.workers)
        enrich_seconds = time.perf_counter() - enrich_started
    finally:
        cache.close()
    log.info("OMDb results: %s | api calls=%d, cache hits=%d, %.1fs with %d worker(s)",
             status_counts, client.stats.api_calls, client.stats.cache_hits, enrich_seconds, cfg.workers)

    # ------------------------------------------------------------------- Load
    conn = connect(cfg.db_path)
    apply_schema(conn)
    metrics = {**status_counts, "api_calls": client.stats.api_calls}
    try:
        with conn:  # single transaction: all or nothing
            metrics["movies_loaded"] = load_movies(conn, movies)
            load_genres(conn, movie_genres, movies["movie_id"].tolist())
            load_directors(conn, movie_directors)
            metrics["ratings_loaded"] = load_ratings(conn, ratings)
    except Exception:
        conn.close()
        raise
    pending = status_counts.get("skipped", 0) + status_counts.get("error", 0)
    exported = export_enriched_csv(conn, cfg.export_path)
    conn.close()
    metrics.update(extract_stats, exported=exported, seconds=time.perf_counter() - started)
    log.info("Exported %d enriched movies to %s", exported, cfg.export_path)

    log.info("Loaded %d movies and %d ratings into %s in %.1fs",
             metrics["movies_loaded"], metrics["ratings_loaded"], cfg.db_path, time.perf_counter() - started)
    if pending:
        log.info("%d movies are not enriched yet; run again (e.g. tomorrow) to continue.", pending)
    return metrics


def main(argv=None) -> int:
    args = parse_args(argv)
    level = args.log_level or "WARNING"
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    cfg = load_config(data_dir=args.data_dir, db_path=args.db_path, cache_path=args.cache_path,
                      max_api_calls=args.max_api_calls, offline=args.offline,
                      workers=args.workers, requests_per_second=args.rps)
    try:
        metrics = run(cfg)
    except OmdbAuthError as exc:
        log.error("%s Check OMDB_API_KEY in your .env file (and that the key is activated).", exc)
        return 2
    except (FileNotFoundError, SchemaMismatch) as exc:
        log.error("%s", exc)
        return 1
    print_pipeline_summary(cfg, metrics)
    print("\nANSWERS")
    print_query_results(cfg.db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
