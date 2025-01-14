{{ config(
    materialized='table'
) }}

WITH distinct_authors AS (
    SELECT DISTINCT
        TRIM(UPPER(author_name)) AS author_name
    FROM {{ ref('int_authors') }}
    WHERE author_name IS NOT NULL
),

author_revenue AS (
    SELECT
        da.author_name,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.volume_sold), 0) AS total_books_sold
    FROM distinct_authors AS da
    LEFT JOIN {{ ref('int_books') }} AS books
        ON TRIM(UPPER(books.book_author)) = da.author_name
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY da.author_name
)

SELECT *
FROM author_revenue
WHERE author_name IS NOT NULL
