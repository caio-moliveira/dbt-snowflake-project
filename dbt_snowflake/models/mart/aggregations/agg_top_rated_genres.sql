{{ config(
    materialized='table'
) }}

SELECT
    books.genre,
    AVG(ratings.rating) AS avg_rating,
    COUNT(ratings.rating) AS rating_count
FROM {{ ref('stg_goodreads') }} ratings
LEFT JOIN {{ ref('stg_google_books') }} books
    ON ratings.ISBN = books.ISBN
GROUP BY books.genre
ORDER BY avg_rating DESC, rating_count DESC
