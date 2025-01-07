-- Test that all book titles in stg_books are also found in stg_google_books
SELECT
    books.title
FROM {{ ref('stg_books') }} books
LEFT JOIN {{ ref('stg_google_books') }} google
ON books.title = google.title
WHERE google.title IS NULL;
