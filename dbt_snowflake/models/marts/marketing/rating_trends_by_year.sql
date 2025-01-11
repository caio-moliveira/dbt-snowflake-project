{{ config(
    materialized='table'
) }}

SELECT
    books.publication_date AS publication_year,
    books.author AS author,
    google.genre AS genre,
    AVG(goodreads.rating) AS avg_rating,
    COUNT(goodreads.rating) AS rating_count
FROM {{ ref('int_goodreads') }} goodreads
LEFT JOIN {{ ref('int_books') }} books
    ON goodreads.ISBN = books.ISBN
LEFT JOIN {{ ref('int_google_books') }} google
    ON books.ISBN = google.ISBN
WHERE publication_year > '1970-01-01' AND publication_year < '2025-01-01'
GROUP BY publication_year, books.author, genre
ORDER BY publication_year, books.author,genre
