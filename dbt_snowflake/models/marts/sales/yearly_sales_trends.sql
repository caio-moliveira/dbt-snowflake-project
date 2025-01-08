{{ config(
    materialized='table',
) }}

WITH yearly_trends AS (
    SELECT
        sales.sales_year,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        COUNT(DISTINCT sales.ISBN) AS unique_books_sold,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_sales') }} AS sales
    GROUP BY sales_year
)

SELECT *
FROM yearly_trends
