{{ config(
    materialized='table'
) }}

SELECT
    books.publisher_group AS publisher,
    books.title AS book_title,
    SUM(sales.Sales_Volume) AS total_volume,
    SUM(sales.Sales_Value) AS total_value
FROM {{ ref('stg_sales') }} sales
LEFT JOIN {{ ref('stg_books') }} books
    ON sales.ISBN = books.ISBN
GROUP BY books.publisher_group, books.title
ORDER BY publisher, total_value DESC
