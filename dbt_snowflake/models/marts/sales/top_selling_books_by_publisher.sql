{{ config(
    materialized='table'
) }}

WITH top_books_by_publisher AS (
    SELECT
        books.publisher_group AS publisher_name,
        books.ISBN,
        books.title,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_books') }} AS books
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY books.publisher_group, books.ISBN, books.title
)

SELECT *
FROM top_books_by_publisher
ORDER BY publisher_name, total_revenue DESC
