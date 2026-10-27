# Movie Data Pipeline

An ETL pipeline that takes the MovieLens "small" dataset (`movies.csv`,
`ratings.csv`), enriches each movie with details from the OMDb API (director,
release date, runtime, language, country, IMDb rating, box office), loads everything into a normalised SQLite
database, and answers four analytical questions in SQL.

```
your-repo-name/
├── etl.py        # the whole pipeline: extract, transform, enrich, load, run queries
├── schema.sql    # CREATE TABLE statements (3NF, constraints, indexes)
├── queries.sql   # the SQL answers to the four questions
├── README.md
```

**Quick start:**

```bash
pip install pandas requests python-dotenv
python etl.py
```

This rebuilds the database from the included CSVs and prints a short report:
what each pipeline stage did, the row count of every table in the data model,
and the answers to the four questions. The OMDb details for the 950 most-rated movies (from the first
real API run) are already in `data/movies.db`, and the pipeline never overwrites
a found OMDb result with an empty one, so Q3 answers straight away. To enrich
more movies, add an OMDb key (below) and run `python etl.py`.

**Why the database holds all 9,742 movies but only 950 are enriched.** The
free OMDb key allows 1,000 calls a day, so the first run enriched the 950
most-rated movies (they carry 59% of all ratings). Q1, Q2 and Q4 use only
MovieLens data, so they are computed over all 9,742 movies and 100,836 ratings;
limiting them to 950 movies would drop 41% of the ratings and bias the answers.
Q3 needs the director, which only OMDb provides, so it uses the 950 enriched
movies. `movies_enriched.csv` contains exactly those 950.

---

## 1. Setup and run

### Requirements

Python 3.10+ and three libraries:

```bash
pip install pandas requests python-dotenv
```

### Data

The MovieLens CSVs are already in `data/raw/`. In general, `etl.py` looks for `movies.csv` and `ratings.csv` (and optionally `links.csv`)
in this order:

1. `data/raw/` next to `etl.py`
2. the same folder as `etl.py`
3. if neither has them, it **downloads** `ml-latest-small.zip` from
   https://grouplens.org/datasets/movielens/latest/ into `data/raw/`
   automatically.


### OMDb API key

Get a free key at https://www.omdbapi.com/apikey.aspx and click the activation
link in the email. Then create a file called `.env` next to `etl.py`:

```
OMDB_API_KEY=your_key_here
```

(or `export OMDB_API_KEY=...` in the shell). The key is **never** written in the
code.

### Run

```bash
python etl.py
```

This extracts, transforms, enriches, loads, and then prints the answer to every
query in `queries.sql`.

| Command | What it does |
|---|---|
| `python etl.py` | Full run, then print the pipeline report and the 4 answers |
| `python etl.py --log-level INFO` | Same, with a log line for every step |
| `python etl.py --offline` | No API calls; rebuild using cached OMDb answers only |
| `python etl.py --workers 8 --rps 10` | Parallel lookups and the shared request rate (defaults) |


Outputs (created on first run):

| Path | Contents |
|---|---|
| `data/movies.db` | The SQLite database |
| `data/cache/omdb_cache.sqlite` | Every OMDb response received so far. Keep it. |
| `data/processed/movies_enriched.csv` | One flat row per OMDb-enriched movie (MovieLens + OMDb fields), handy for Excel |

The queries can also be run by hand: `sqlite3 data/movies.db < queries.sql`.

### The daily API limit

The free OMDb key allows 1,000 requests per day. The pipeline stops at 950 to
leave headroom and enriches the **most-rated movies first**, so the first run
covers the movies behind about 59% of all ratings. Because responses are
cached, simply running `python etl.py` once a day continues where it stopped;
already-fetched movies cost nothing. With `links.csv` the full catalogue of
9,742 movies takes about 11 daily runs. Q1, Q2 and Q4 do not need OMDb at all.

---

## 2. How it works

`etl.py` is organised in eight sections, top to bottom:

| # | Section | Responsibility |
|---|---|---|
| 1 | Configuration | Settings from env vars / `.env`, overridden by command-line flags |
| 2 | Extract | Read the CSVs with explicit dtypes, check columns, auto-download if missing |
| 3 | Transform | Parse titles and years, split genres, validate ratings, parse OMDb fields |
| 4 | OMDb API client | Cached, rate-limited, retrying, thread-safe HTTP client |
| 5 | Enrichment | Look up all movies in parallel, most-rated first |
| 6 | Load | Apply `schema.sql`, upsert all tables in one transaction, export CSV |
| 7 | Analytical queries | Run `queries.sql` and print the results |
| 8 | Orchestration | `run()` ties the steps together and prints the report |

**Extract.** pandas reads the files with fixed dtypes, so a malformed file fails
immediately with a clear message instead of being silently mis-parsed.

**Transform.** MovieLens titles look like `"Matrix, The (1999)"` or
`"Seven (a.k.a. Se7en) (1995)"`. The transform extracts the year, moves trailing
articles back to the front (`"The Matrix"`), keeps alternate titles for search,
splits the pipe-separated genres, and drops ratings outside 0.5 to 5.0 or for
unknown movies. OMDb fields arrive as strings (`"$292,587,330"`, `"136 min"`,
`"N/A"`), so they are converted to numbers and `NULL`s.

**Enrich.** For each movie the client tries, in order: the exact IMDb id (from
`links.csv`), then title + year, then title alone (accepted only if OMDb's year
is within one year), then any alternate title. Every answer, including "movie
not found", is cached in SQLite, so no request is ever paid for twice.

**Load.** `schema.sql` is applied with `CREATE TABLE IF NOT EXISTS`, then every
table is written with `INSERT ... ON CONFLICT DO UPDATE` keyed on natural ids.
The whole load runs in one transaction.

---

## 3. Data model

Only the source columns are stored. Nothing else is added.

| Source | Columns used |
|---|---|
| `movies.csv` | `movieId`, `title`, `genres` |
| `ratings.csv` | `userId`, `movieId`, `rating`, `timestamp` |
| OMDb API | `imdbID`, `Director`, `Released`, `Runtime`, `Language`, `Country`, `imdbRating`, `BoxOffice` |

```
movies 1──* ratings
movies 1──* movie_genres    *──1 genres
movies 1──* movie_directors *──1 directors
```

| Table | Key | Columns |
|---|---|---|
| `movies` | `movie_id` | `title`, `release_year` (from the title), `imdb_id`, `released_date`, `runtime_minutes`, `language`, `country`, `imdb_rating`, `box_office_usd` |
| `ratings` | (`user_id`, `movie_id`) | `rating` (`CHECK 0.5–5.0`), `timestamp` |
| `genres`, `movie_genres` | `genre_id`; (`movie_id`, `genre_id`) | the `genres` column split on `|` |
| `directors`, `movie_directors` | `director_id`; (`movie_id`, `director_id`) | the OMDb `Director` field split on `,` |

Genres and directors are stored in their own tables instead of as text like
`"Adventure|Animation"`, so "average rating per genre" (Q2) and "movies per
director" (Q3) are simple, correct joins, and a co-directed movie counts for
each director. Indexes cover every join column. The OMDb columns in `movies`
stay `NULL` for movies that have not been enriched yet.

---

## 4. Design choices

* **SQLite** needs no server, so the project runs anywhere with one command. The
  SQL is standard (`ON CONFLICT` upserts, `CHECK`s, foreign keys) and moves to
  PostgreSQL with only type changes.
* **Idempotent.** Rerunning updates rows in place and never duplicates them.
  Genre and director rows for a movie are refreshed together, and the OMDb
  columns are updated with `COALESCE`, so a run that did not look a movie up
  (offline, or after the daily limit) never erases data from an earlier run.
* **Parallel but polite.** API calls are I/O-bound, so a thread pool (8 workers)
  overlaps the waiting. One lock hands out evenly spaced start times (max 10
  requests/second across all threads) and reserves each call against the budget
  *before* sending it, so the run can never exceed 950 calls. In a benchmark with
  a simulated 300 ms API, 100 movies took 30.3 s sequentially and 10.2 s in
  parallel.
* **Resilient.** Retries with exponential backoff on network errors, HTTP 429
  and 5xx; a clean stop on "Request limit reached!"; an invalid key stops the
  run with a clear message rather than marking every movie as not found.
* **Secrets out of code.** The key is read from the environment or `.env` only.

---

## 5. Assumptions

* The MovieLens "small" dataset (9,742 movies, 100,836 ratings) is the input.
* The release year comes from the MovieLens title. For the 12 titles without
  one, OMDb's year is used as a fallback.
* Q1 counts only movies with at least 50 ratings. Without that minimum, a film
  with a single 5-star rating would have the "highest average" (5.0).
* Q2 averages individual ratings per genre; a movie counts toward each of its
  genres.
* Q3 counts distinct movies per director using the `movie_directors` bridge; a
  co-directed film counts for each director. Only movies OMDb has returned so
  far are counted.
* Q4 averages every rating of the movies released in each year.
* Movies OMDb cannot find stay in the database with empty OMDb columns. The
  "not found" answer is cached, so they are not looked up again.

## 6. Challenges and how they were solved

| Challenge | Solution |
|---|---|
| 1,000 API calls/day vs 9,742 movies | Persistent cache, 950-call budget, most-rated-first ordering, daily resumable runs |
| Titles that don't match OMDb (`"Matrix, The"`, `a.k.a.` titles, wrong years) | Exact IMDb-id lookup first, then a fallback chain with article fixing and a ±1-year check |
| Slow sequential API calls | Thread pool with a shared rate limiter and atomic budget |
| OMDb returns `"N/A"` and formatted strings | Typed parsing to numbers and `NULL` |
| numpy integers stored as BLOBs by sqlite3 | Convert to native Python types before insert |
| Reruns creating duplicates | Natural keys + `ON CONFLICT DO UPDATE`, one transaction |

---

## 7. Results

From the first daily run (950 most-rated movies enriched):

| Question | Answer |
|---|---|
| Q1 Highest average rating | **The Shawshank Redemption (1994)**: 4.43 from 317 ratings |
| Q2 Top 5 genres | Film-Noir 3.92, War 3.81, Documentary 3.80, Crime 3.66, Drama 3.66 |
| Q3 Top director | **Steven Spielberg, 17 movies**, ahead of Tim Burton (13) and David Fincher (10) |
| Q4 Rating by year | 106 years. Films from before 1980 average about 3.80; films since, about 3.46 |

---


To take it to production:

* Move to PostgreSQL and schedule the daily run with Airflow.
* Use the IMDb public datasets for bulk static fields, and the API only for
  updates.
* Add data-quality checks (row counts, null rates) as a gate before publishing.
* Load ratings incrementally by timestamp instead of re-reading the full file.
