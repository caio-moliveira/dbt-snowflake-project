{{ config(
    materialized='table'
) }}

SELECT
    sales.ISBN,
    books.title AS book_title,
    sales.sales_year AS sales_year,
    SUM(sales.sales_volume) AS total_volume,
    SUM(sales.sales_value) AS total_value,
    AVG(sales.average_sales_price) AS avg_selling_price
FROM {{ ref('stg_sales') }} sales
LEFT JOIN {{ ref('stg_books') }} books
    ON sales.ISBN = books.ISBN
GROUP BY sales.ISBN, books.title, sales.sales_year
