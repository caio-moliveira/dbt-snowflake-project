{{ config(
    materialized='table'
) }}

WITH publisher_revenue AS (
    SELECT
        books.publisher_group AS publisher_name,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_books') }} AS books
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY books.publisher_group
)

SELECT *
FROM publisher_revenue
