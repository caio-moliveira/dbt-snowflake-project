{{ config(
    materialized='table'
) }}

SELECT
    books.title AS book_title,
    books.author,
    AVG(ratings.rating) AS avg_rating,
    COUNT(ratings.rating) AS rating_count
FROM {{ ref('stg_goodreads') }} ratings
LEFT JOIN {{ ref('stg_books') }} books
    ON ratings.ISBN = books.ISBN
GROUP BY books.title, books.author
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 100