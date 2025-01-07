{{ config(
    materialized='table'
) }}

SELECT
    books.publication_date AS publication_year,
    AVG(goodreads.rating) AS avg_rating,
    COUNT(goodreads.rating) AS rating_count
FROM {{ ref('stg_goodreads') }} goodreads
LEFT JOIN {{ ref('stg_books') }} books
    ON goodreads.ISBN = books.ISBN
GROUP BY publication_year
ORDER BY publication_year
