-- =============================================================================
-- queries.sql : the four analytical questions
-- =============================================================================
-- Run with:  python etl.py                      (prints the answers at the end)
--       or:  sqlite3 data/movies.db < queries.sql
-- =============================================================================


-- Q1. Which movie has the highest average rating?
--     Only movies with at least 50 ratings count; otherwise a film with a single
--     5-star rating would win.
SELECT m.title,
       ROUND(AVG(r.rating), 2) AS avg_rating,
       COUNT(*)                AS num_ratings
FROM ratings r
JOIN movies  m ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
HAVING COUNT(*) >= 50
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;


-- Q2. What are the top 5 genres with the highest average rating?
SELECT g.name                  AS genre,
       ROUND(AVG(r.rating), 2) AS avg_rating
FROM ratings r
JOIN movie_genres mg ON mg.movie_id = r.movie_id
JOIN genres g        ON g.genre_id  = mg.genre_id
GROUP BY g.genre_id, g.name
ORDER BY avg_rating DESC
LIMIT 5;


-- Q3. Who is the director with the most movies in this dataset?
SELECT d.name                      AS director,
       COUNT(DISTINCT md.movie_id) AS num_movies
FROM movie_directors md
JOIN directors d ON d.director_id = md.director_id
GROUP BY d.director_id, d.name
ORDER BY num_movies DESC, d.name
LIMIT 1;


-- Q4. What is the average rating of movies released each year?
SELECT m.release_year,
       ROUND(AVG(r.rating), 2) AS avg_rating
FROM ratings r
JOIN movies m ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;
