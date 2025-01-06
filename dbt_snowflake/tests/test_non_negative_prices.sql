-- Test that all prices are non-negative across all staging tables
SELECT *
FROM (
    SELECT 'amazon_books' AS source, price FROM {{ ref('stg_amazon_books') }}
    UNION ALL
    SELECT 'google_books' AS source, price FROM {{ ref('stg_google_books') }}
    UNION ALL
    SELECT 'mercado_livre_books' AS source, price FROM {{ ref('stg_mercadolivre_books') }}
    UNION ALL
    SELECT 'books_store' AS source, price FROM {{ ref('stg_books_store') }}
) t
WHERE price < 0
