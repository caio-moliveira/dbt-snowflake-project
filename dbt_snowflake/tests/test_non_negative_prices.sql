-- Test that all prices are non-negative across all staging tables
SELECT *
FROM (
    SELECT 'books' AS source, price FROM {{ ref('stg_books') }}
    UNION ALL
    SELECT 'google_books' AS source, price FROM {{ ref('stg_google_books') }}
    UNION ALL
    SELECT 'sales' AS source, price FROM {{ ref('stg_sales') }}
) t
WHERE price < 0;
