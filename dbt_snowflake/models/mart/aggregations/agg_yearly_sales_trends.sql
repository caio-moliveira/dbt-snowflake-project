{{ config(
    materialized='table'
) }}

SELECT
    sales.sales_year AS sales_year,
    SUM(sales.sales_volume) AS total_volume,
    SUM(sales.sales_value) AS total_value
FROM {{ ref('stg_sales') }} sales
GROUP BY sales.sales_year
ORDER BY sales_year
