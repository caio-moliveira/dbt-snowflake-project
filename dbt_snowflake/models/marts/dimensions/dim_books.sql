{{ config(
    materialized='table'
) }}

WITH books_metadata AS (
    SELECT
        books.ISBN,
        books.title,
        books.author,
        books.publisher_group,
        google_books.genre AS genre,
        goodreads.rating AS goodreads_rating,
        goodreads.pages AS goodreads_pages,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_books') }} AS books
    LEFT JOIN {{ ref('int_google_books') }} AS google_books
        ON books.ISBN = google_books.ISBN
    LEFT JOIN {{ ref('int_goodreads') }} AS goodreads
        ON books.ISBN = goodreads.ISBN
)

SELECT *
FROM books_metadata
