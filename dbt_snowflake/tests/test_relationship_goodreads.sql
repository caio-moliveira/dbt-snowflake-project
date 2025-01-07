-- Test that all book titles in stg_books are also found in stg_google_books
SELECT
    books.title
FROM {{ ref('stg_books') }} books
LEFT JOIN {{ ref('stg_goodreads') }} goodreads
ON books.title = goodreads.title
WHERE goodreads.title IS NULL;
