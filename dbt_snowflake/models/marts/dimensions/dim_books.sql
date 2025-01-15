{{ config(
    materialized='table'
) }}

WITH deduplicated_books AS (
    SELECT DISTINCT ISBN, book_title, book_author
    FROM {{ ref('int_books') }}
),
deduplicated_google_books AS (
    SELECT DISTINCT ISBN, gb_genre
    FROM {{ ref('int_google_books') }}
),
deduplicated_goodreads AS (
    SELECT DISTINCT ISBN, gr_rating, gr_pages
    FROM {{ ref('int_goodreads') }}
),
books_metadata AS (
    SELECT
        books.ISBN,
        books.book_title,
        google_books.gb_genre AS genre,
        ROUND(AVG(goodreads.gr_rating),2) AS rating,
        goodreads.gr_pages AS pages
    FROM deduplicated_books AS books
    LEFT JOIN deduplicated_google_books AS google_books
        ON books.ISBN = google_books.ISBN
    LEFT JOIN deduplicated_goodreads AS goodreads
        ON books.ISBN = goodreads.ISBN
    GROUP BY books.ISBN, books.book_title, google_books.gb_genre, goodreads.gr_pages
)
SELECT *
FROM books_metadata
WHERE genre IS NOT NULL AND pages IS NOT NULL
