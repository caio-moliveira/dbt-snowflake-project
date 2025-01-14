{{ config(
    materialized='table'
) }}

WITH author_sales AS (
    SELECT
        authors.author_id AS author_id,
        authors.author_name AS author_name,
        authors.publisher_group AS publisher_group,
        COALESCE(SUM(sales.volume_sold), 0) AS total_books_sold,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
    FROM {{ ref('int_authors') }} AS authors
    LEFT JOIN {{ ref('int_books') }} AS books
        ON authors.author_name = books.book_author
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY authors.author_id, authors.author_name, authors.publisher_group
)

SELECT *
FROM author_sales
