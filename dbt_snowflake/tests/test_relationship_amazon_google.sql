-- Test that all book titles in stg_amazon_books are also found in stg_google_books
SELECT
    amazon.title
FROM {{ ref('stg_amazon_books') }} amazon
LEFT JOIN {{ ref('stg_google_books') }} google
ON amazon.title = google.title
WHERE google.title IS NULL
