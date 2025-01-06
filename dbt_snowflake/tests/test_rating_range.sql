-- Test that ratings are within the valid range (0 to 5)
SELECT *
FROM (
    SELECT 'google_books' AS source, average_rating AS rating FROM {{ ref('stg_google_books') }}
    UNION ALL
    SELECT 'amazon_books' AS source, average_rating AS rating FROM {{ ref('stg_amazon_books') }}
    UNION ALL
    SELECT 'mercadolivre_books' AS source, review AS rating FROM {{ ref('stg_mercadolivre_books') }}
    UNION ALL
    SELECT 'books_store' AS source, star_rating::FLOAT AS rating FROM {{ ref('stg_books_store') }}
) t
WHERE rating < 0 OR rating > 5;
