{{ config(
    materialized='table'
) }}

WITH top_books_by_publisher AS (
    SELECT
        authors.publisher_group AS publisher_name,
        books.ISBN,
        books.book_title,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.volume_sold), 0) AS total_books_sold
    FROM {{ ref('int_authors') }} AS authors
    LEFT JOIN {{ ref('int_books') }} AS books
        ON authors.author_name = books.book_author
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY authors.publisher_group, books.ISBN, books.book_title
)

SELECT *
FROM top_books_by_publisher
ORDER BY publisher_name, total_revenue DESC
