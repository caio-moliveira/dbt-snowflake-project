{{ config(
    materialized='table'
) }}

SELECT
    books.publisher_group AS publisher,
    SUM(sales.Sales_Value) AS total_revenue
FROM {{ ref('stg_sales') }} sales
LEFT JOIN {{ ref('stg_books') }} books
    ON sales.ISBN = books.ISBN
GROUP BY books.publisher_group
ORDER BY total_revenue DESC
