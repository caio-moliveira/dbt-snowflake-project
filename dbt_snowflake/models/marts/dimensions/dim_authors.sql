{{ config(
    materialized='table'
) }}

WITH author_sales AS (
    SELECT
        authors.author_id AS author_id,
        authors.author_name AS author_name,
        books.publisher_group AS publisher_group,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_author') }} AS authors
    LEFT JOIN {{ ref('int_books') }} AS books
        ON authors.author_name = books.author
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY authors.author_id, authors.author_name, books.publisher_group
)

SELECT *
FROM author_sales
