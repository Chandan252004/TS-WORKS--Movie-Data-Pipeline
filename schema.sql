-- =============================================================================
-- schema.sql : relational model for the MovieLens + OMDb movie pipeline
-- =============================================================================
-- Target database: SQLite (the same SQL runs on PostgreSQL with minor type
-- changes, e.g. INTEGER PRIMARY KEY AUTOINCREMENT -> SERIAL).
--
-- Sources:
--   movies.csv  : movieId, title, genres
--   ratings.csv : userId, movieId, rating, timestamp
--   OMDb API    : imdbID, Director, Released, Runtime, Language, Country,
--                 imdbRating, BoxOffice
--
--   movies 1---* ratings
--   movies 1---* movie_genres    *---1 genres      (from the genres column)
--   movies 1---* movie_directors *---1 directors   (from the OMDb Director field)
--
-- Every statement uses IF NOT EXISTS, so etl.py can apply this file on every run.
-- =============================================================================

PRAGMA foreign_keys = ON;

-- One row per movie: MovieLens fields plus the OMDb fields we keep.
-- OMDb columns stay NULL for movies that have not been enriched yet.
CREATE TABLE IF NOT EXISTS movies (
    movie_id         INTEGER PRIMARY KEY,   -- movieId
    title            TEXT    NOT NULL,      -- title, as in movies.csv
    release_year     INTEGER,               -- the "(1995)" at the end of title
    imdb_id          TEXT,                  -- OMDb imdbID, e.g. "tt0114709"
    released_date    TEXT,                  -- OMDb Released, as ISO date "1995-11-22"
    runtime_minutes  INTEGER,               -- OMDb Runtime, "81 min" -> 81
    language         TEXT,                  -- OMDb Language
    country          TEXT,                  -- OMDb Country
    imdb_rating      REAL,                  -- OMDb imdbRating, 0.0 - 10.0
    box_office_usd   INTEGER                -- OMDb BoxOffice, "$394,436,586" -> 394436586
);

CREATE INDEX IF NOT EXISTS idx_movies_release_year ON movies (release_year);

-- genres: the "Adventure|Animation|Comedy" column split into one row per genre.
CREATE TABLE IF NOT EXISTS genres (
    genre_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id   INTEGER NOT NULL REFERENCES movies (movie_id) ON DELETE CASCADE,
    genre_id   INTEGER NOT NULL REFERENCES genres (genre_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_movie_genres_genre ON movie_genres (genre_id);

-- directors: the OMDb Director field ("Lana Wachowski, Lilly Wachowski") split
-- into one row per director, so a co-directed movie counts for each of them.
CREATE TABLE IF NOT EXISTS directors (
    director_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id     INTEGER NOT NULL REFERENCES movies (movie_id) ON DELETE CASCADE,
    director_id  INTEGER NOT NULL REFERENCES directors (director_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, director_id)
);

CREATE INDEX IF NOT EXISTS idx_movie_directors_director ON movie_directors (director_id);

-- ratings: exactly the four columns of ratings.csv. (user_id, movie_id) is the
-- natural key, so re-running the pipeline updates rows instead of duplicating them.
CREATE TABLE IF NOT EXISTS ratings (
    user_id     INTEGER NOT NULL,                                        -- userId
    movie_id    INTEGER NOT NULL REFERENCES movies (movie_id) ON DELETE CASCADE,  -- movieId
    rating      REAL    NOT NULL CHECK (rating BETWEEN 0.5 AND 5.0),     -- rating
    timestamp   INTEGER NOT NULL,                                        -- timestamp (Unix seconds)
    PRIMARY KEY (user_id, movie_id)
);

CREATE INDEX IF NOT EXISTS idx_ratings_movie ON ratings (movie_id);
