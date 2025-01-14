{{ config(
    materialized='table'
) }}

WITH author_sales AS (
    SELECT
        authors.publisher_group,
        ROUND(authors.avg_rating, 2) AS avg_rating,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.volume_sold), 0) AS total_books_sold
    FROM {{ ref('int_books') }} AS books
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    LEFT JOIN {{ ref('int_authors') }} AS authors
        ON books.book_author = authors.author_name
    WHERE authors.author_name IS NOT NULL
    GROUP BY
        authors.publisher_group,
        authors.avg_rating
),
ranked_authors AS (
    SELECT
        publisher_group,
        avg_rating,
        total_revenue,
        total_books_sold,
        ROW_NUMBER() OVER (
            PARTITION BY publisher_group
            ORDER BY total_revenue DESC
        ) AS rank
    FROM author_sales
)

SELECT
    publisher_group,
    avg_rating,
    total_revenue,
    total_books_sold
FROM ranked_authors
WHERE rank = 1
ORDER BY total_revenue DESC
LIMIT 50
