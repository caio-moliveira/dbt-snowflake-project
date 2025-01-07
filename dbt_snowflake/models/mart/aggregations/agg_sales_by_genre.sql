{{ config(
    materialized='table'
) }}

SELECT
    books.genre,
    SUM(sales.Sales_Volume) AS total_volume,
    SUM(sales.Sales_Value) AS total_value
FROM {{ ref('stg_sales') }} sales
LEFT JOIN {{ ref('stg_google_books') }} books
    ON sales.ISBN = books.ISBN
GROUP BY books.genre
ORDER BY total_value DESC