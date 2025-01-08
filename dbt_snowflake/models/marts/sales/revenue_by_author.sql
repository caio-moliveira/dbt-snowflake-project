{{ config(
    materialized='table'
) }}

WITH author_revenue AS (
    SELECT
        authors.author_id,
        authors.author_name,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_books') }} AS books
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    LEFT JOIN {{ ref('int_author') }} AS authors
        ON books.author = authors.author_name
    GROUP BY authors.author_id, authors.author_name
)

SELECT *
FROM author_revenue
