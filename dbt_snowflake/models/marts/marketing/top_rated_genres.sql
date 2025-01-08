{{ config(
    materialized='table'
) }}

WITH genre_ratings AS (
    SELECT
        google_books.genre AS genre,
        AVG(goodreads.rating) AS avg_rating,
        COUNT(DISTINCT books.ISBN) AS total_books,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_google_books') }} AS google_books
    LEFT JOIN {{ ref('int_books') }} AS books
        ON google_books.ISBN = books.ISBN
    LEFT JOIN {{ ref('int_goodreads') }} AS goodreads
        ON books.ISBN = goodreads.ISBN
    WHERE goodreads.rating IS NOT NULL
    GROUP BY genre
)

SELECT *
FROM genre_ratings
ORDER BY avg_rating DESC, total_books DESC
LIMIT 50
