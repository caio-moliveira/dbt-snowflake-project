{{ config(
    materialized='table'
) }}

WITH rated_books AS (
    SELECT
        books.ISBN,
        books.title,
        books.author,
        goodreads.rating,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_books') }} AS books
    LEFT JOIN {{ ref('int_goodreads') }} AS goodreads
        ON books.ISBN = goodreads.ISBN
    WHERE goodreads.rating IS NOT NULL
)

SELECT *
FROM rated_books
ORDER BY rating DESC
LIMIT 100
