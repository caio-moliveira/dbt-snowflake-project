{{ config(
    materialized='table'
) }}

WITH books_metadata AS (
    SELECT
        books.ISBN,
        books.book_title,
        books.book_author,
        authors.publisher_group,
        google_books.gb_genre AS genre,
        goodreads.gr_rating AS rating,
        goodreads.gr_pages AS pages,
    FROM {{ ref('int_authors') }} AS authors
    LEFT JOIN {{ ref('int_books') }} AS books
        ON authors. author_name = books.book_author
    LEFT JOIN {{ ref('int_google_books') }} AS google_books
        ON books.ISBN = google_books.ISBN
    LEFT JOIN {{ ref('int_goodreads') }} AS goodreads
        ON books.ISBN = goodreads.ISBN
)

SELECT *
FROM books_metadata
