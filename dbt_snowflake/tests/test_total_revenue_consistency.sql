-- Test that total revenue matches between staging and mart layers
WITH staging_revenue AS (
    SELECT SUM(price) AS total_revenue
    FROM {{ ref('stg_sales') }}
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
