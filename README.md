# Building a Movie Data Pipeline

A small ETL pipeline that ingests movie data from two sources (the MovieLens dataset and the OMDb API), cleans and transforms it, loads it into MySQL, and answers analytical questions with SQL.

## Overview of the solution

The pipeline follows a **medallion architecture**:

| Layer | What it holds | Where |
|---|---|---|
| **Bronze** | The OMDb-enriched dataset, exactly as received (stored as text) so the original can always be recovered | `movie_bronze` schema, `data/output/bronze/` |
| **Silver** | The enriched dataset after cleaning, transformation and missing-value handling | `movie_silver` schema, `data/output/silver/` |
| **Gold** | A relational model with primary and foreign keys, used for the analytical queries | `movie_gold` schema, `data/output/gold/` |

**Sources**

* **MovieLens** (`movies.csv`, `ratings.csv`, `links.csv`): 9,742 movies and 100,836 ratings. `links.csv` holds each movie's IMDb ID, which is the key used to call OMDb.
* **OMDb API**: adds **Director**, **Plot** and **Box Office earnings** for each movie, looked up by IMDb ID. The enriched result is `final_dataset.csv` (2,875 movies).

**Silver: cleaning** (in this order)
1. Drop duplicate rows and duplicate `movieId` values (none found)
2. Drop unused columns (none: all 8 columns are used)
3. Remove special characters that carry no value (a stray `` ` `` in box office, `~` used as a dash, extra spaces)
4. Turn every null-like value (`NaN`, `N/A`, `null`, `None`, `-`) into a real null: blank in CSV, `NULL` in MySQL
5. One format per column: `"$3,041,083"` → `3041083`, ratings to 2 decimals, `"Godfather, The (1972)"` → `"The Godfather (1972)"`

**Silver: transformations** (each new column sits beside its original)
1. Box office USD → INR at a fixed rate (1 USD = ₹88), in the Indian comma format: `$3,041,083` → `26,76,15,304`
2. `title` split into **Movie Name** and an integer **Release Year**: `Toy Story (1995)` → `Toy Story` | `1995`
3. Unix timestamp → readable UTC datetime: `1537799250` → `2018-09-24 14:27:30`

**Gold tables** (`schema.sql`, ER diagram in [`docs/er-diagram.png`](docs/er-diagram.png))

| Table | Key | Rows |
|---|---|---|
| `movie` | `movie_id` | 9,742 |
| `rating` | (`user_id`, `movie_id`), FK `movie_id` | 100,836 |
| `links` | `imdb_id`, FK `movie_id` (unique, 1 : 1 with movie) | 9,742 |
| `final` | `movie_id` (FK to movie): the transformed, enriched dataset | 2,871 |
| `movie_director` | (`movie_id`, `director`): co-directors split into one row each, for the director question | 3,047 |

**Analytical questions** (`queries.sql`, answered from the transformed `final` table)

| Question | Answer |
|---|---|
| Highest average rating | 40 movies tie at 5.0 (e.g. *Lamerica*, 1994) |
| Top 5 genres by average rating | IMAX 3.93, Film-Noir 3.74, Documentary 3.71, War 3.54, Mystery 3.47 |
| Director with the most movies | Alfred Hitchcock, 36 |
| Average rating by release year | one row per year, 1919 to 2000 |

## Repository structure

```
movie-data-pipeline/
├── etl.py                      # the full pipeline as a script
├── schema.sql                  # CREATE TABLE statements for the Gold layer
├── queries.sql                 # the analytical questions
├── README.md
├── requirements.txt
├── .env.example                # settings read by etl.py
├── notebook/DataPipeline.ipynb # the same pipeline step by step, with outputs and charts
├── docs/er-diagram.png         # ER diagram
└── data/                       # put the input CSV files here (see data/README.md)
```

## Setup and run

**Prerequisites:** Python 3.10+ and MySQL 8 (running locally).

1. Clone the repository and install the libraries:
   ```bash
   pip install -r requirements.txt
   ```
2. Put `movies.csv`, `ratings.csv`, `links.csv` and `final_dataset.csv` in the `data/` folder.
3. Set your MySQL connection (and, optionally, your OMDb key). The URL has no database name; the pipeline creates `movie_bronze`, `movie_silver` and `movie_gold` itself.
   ```bash
   # macOS / Linux
   export MYSQL_URL="mysql+pymysql://root:your_password@localhost:3306"
   export OMDB_API_KEY="your_omdb_key"

   # Windows PowerShell
   $env:MYSQL_URL="mysql+pymysql://root:your_password@localhost:3306"
   $env:OMDB_API_KEY="your_omdb_key"
   ```
   If your password contains special characters such as `@`, URL-encode them (`@` → `%40`).
4. Run the pipeline:
   ```bash
   python etl.py                  # Bronze -> Silver -> Gold, then runs queries.sql
   python etl.py --omdb-demo      # also shows live OMDb calls for 5 movies
   python etl.py --collect-omdb   # collects one batch (up to 1,000 calls) of OMDb data
   ```
5. To walk through it step by step with charts, open `notebook/DataPipeline.ipynb` in Jupyter, set your MySQL password in cell 1.3, and run **Kernel → Restart & Run All**.

The script can be run any number of times: Bronze and Silver are replaced, and the Gold load is idempotent (it prints `idempotent: True` after loading twice).

## Design choices and assumptions

* **Medallion layers.** Bronze is never modified, so Silver and Gold can always be rebuilt from it.
* **OMDb lookup by IMDb ID, not title.** MovieLens titles look like `Godfather, The (1972)` and often fail a title search. The ID from `links.csv` is exact; title + year is used only as a fallback.
* **Only the enriched dataset is cleaned in Silver.** `movies.csv`, `ratings.csv` and `links.csv` are already clean, so Gold only renames and types them.
* **New columns beside the originals.** Each transformation adds its result next to the source column, so before and after can be compared.
* **Box office in INR at a fixed rate** (`USD_TO_INR = 88.0`, configurable). Silver keeps the Indian-format text; Gold also stores `box_office_inr` as a number so SQL can sum and average it.
* **Composite key on `rating`** (`user_id`, `movie_id`): one rating per user per movie, which also prevents duplicate loads.
* **`imdb_id` is the primary key of `links`**, with `movie_id` as a unique foreign key so movie : links stays 1 : 1.
* **Idempotent load.** One transaction deletes the Gold rows (child tables first) and inserts them again (parent tables first). If anything fails, everything rolls back.
* **Times are in UTC.**

## Challenges and how they were handled

### 1. OMDb API limit: 1,000 calls per day

The free OMDb key allows only 1,000 requests a day, so data could not be collected for all 9,742 movies in one run. It was downloaded **batch-wise**:

* Each day's run fetched up to 1,000 movies and saved the results, then the next run continued from where the previous one stopped (`python etl.py --collect-omdb` appends to `data/omdb_cache.csv` and skips movies already fetched).
* When the key returns "limit reached", the batch stops cleanly instead of failing.
* Requests are retried with back-off (2 s, 4 s) on network errors, with a short delay between calls. "Movie not found" and `N/A` values are stored as `NULL` instead of stopping the run.
* As a result, **2,875 of the 9,742 movies** are enriched. All 9,742 movies and 100,836 ratings are still loaded into Gold.

### 2. Missing values

**Rule of thumb:** under 5% missing → drop or fill simply; over 5% → needs a deliberate strategy.

| Column | Missing | Decision |
|---|---|---|
| Rating and timestamp | 4 movies (0.14%) | **Dropped**: nobody rated these movies, so the rows have no analytical value |
| Director | 33 (1.15%) | **Kept blank**: the rest of the row is still useful |
| Plot | 35 (1.22%) | **Kept blank** |
| Box office | 475 of 2,871 (16.5%) | **Flag and retain** (below) |

**Why box office is missing.** Older films simply never had their earnings documented: more than half of the films before 1970 have no figure, compared with about 6.5% for the 1980s and 1990s. The gaps depend on release year, so they are **not missing at random**.

**Mean and median imputation were tested and rejected:**
* The same value goes into all 475 gaps, creating an artificial spike.
* Variance drops by 16.6% (mean) and 15.0% (median), so the data looks more certain than it is.
* The gaps are mostly old films, so one global value is wrong for exactly those movies.

**Solution: flag and retain.** A `box_office_available` column (1 = figure present, 0 = missing) is added and missing values stay `NULL`. SQL's `AVG()` and `SUM()` skip NULLs, so averages stay honest; filling the gaps with 0 instead would understate the average box office by about 17%.

### 3. Messy values in the enriched file

The enriched file had box office values written as text (`"$3,041,083"`) and one stray `` ` `` instead of a figure, and CSVs saved from Excel can carry a hidden byte-order mark in the first column name. These are handled in the pipeline: the file is read with `utf-8-sig`, column names are stripped, symbol-only cells become null, and `$` and commas are removed before converting to numbers.

## Possible improvements

* Schedule the run (Airflow or cron) and fetch OMDb data incrementally, only for new movies.
* Backfill missing box office figures from a second source, such as TMDB's `revenue` field via `tmdbId`.
* Replace the fixed exchange rate with a currency-rate table.
* Add automated data-quality checks and alerts on the missing-value rate.
