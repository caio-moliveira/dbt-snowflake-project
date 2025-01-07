{{ config(
    materialized='table'
) }}

SELECT
    sales.sales_year AS sales_year,
    books.title AS book_title,
    books.author,
    SUM(sales.sales_volume) AS total_volume,
    SUM(sales.sales_value) AS total_value
FROM {{ ref('stg_sales') }} sales
LEFT JOIN {{ ref('stg_books') }} books
    ON sales.ISBN = books.ISBN
GROUP BY sales.sales_year, books.title, books.author
ORDER BY sales_year, total_value DESC
