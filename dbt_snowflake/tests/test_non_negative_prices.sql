-- Test that all prices are non-negative across all staging tables
SELECT *
FROM {{ ref('int_sales') }}
WHERE recommended_retail_price < 0 OR average_sales_price < 0
