{{ config(
    materialized='table'
) }}

SELECT
    author,
    COUNT(DISTINCT books.ISBN) AS total_books,
    AVG(fact_sales.total_value) AS avg_sales_value
FROM {{ ref('stg_books') }} books
LEFT JOIN {{ ref('fact_sales') }} fact_sales
    ON books.ISBN = fact_sales.ISBN
GROUP BY author
