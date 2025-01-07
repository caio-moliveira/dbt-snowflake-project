{{ config(
    materialized='table'
) }}

SELECT
    books.author,
    SUM(sales.Sales_Volume) AS total_volume,
    SUM(sales.Sales_Value) AS total_value
FROM {{ ref('stg_sales') }} sales
LEFT JOIN {{ ref('stg_books') }} books
    ON sales.ISBN = books.ISBN
GROUP BY books.author
ORDER BY total_value DESC
