{{ config(
    materialized='table'
) }}

WITH yearly_sales AS (
    SELECT
        sales.sales_year,
        books.ISBN,
        books.title,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_books') }} AS books
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY sales_year, books.ISBN, books.title
)

SELECT *
FROM yearly_sales
ORDER BY sales_year, total_revenue DESC
