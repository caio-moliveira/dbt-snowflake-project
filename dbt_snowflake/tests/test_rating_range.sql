-- Test that ratings are within the valid range (0 to 5)
SELECT *
FROM (
    SELECT 'google_books' AS source, rating FROM {{ ref('stg_google_books') }}
    UNION ALL
    SELECT 'goodreads' AS source, rating FROM {{ ref('stg_goodreads') }}
) t
WHERE rating < 0 OR rating > 5;

