-- Test that total revenue matches between staging and mart layers
WITH staging_revenue AS (
    SELECT SUM(price) AS total_revenue
    FROM (
        SELECT price FROM {{ ref('stg_amazon_books') }}
        UNION ALL
        SELECT price FROM {{ ref('stg_google_books') }}
        UNION ALL
        SELECT price FROM {{ ref('stg_mercadolivre_books') }}
        UNION ALL
        SELECT price FROM {{ ref('stg_books_store') }}
    )
),
mart_revenue AS (
    SELECT SUM(price) AS total_revenue
    FROM {{ ref('fact_books_sales') }}
)
SELECT
    staging_revenue.total_revenue AS staging_revenue,
    mart_revenue.total_revenue AS mart_revenue
FROM staging_revenue, mart_revenue
WHERE staging_revenue.total_revenue != mart_revenue.total_revenue;
