{{ config(
    materialized='table'
) }}

WITH distinct_authors AS (
    SELECT DISTINCT
        TRIM(UPPER(author_name)) AS author_name  -- Normalize author names
    FROM {{ ref('int_author') }}
    WHERE author_name IS NOT NULL
),

author_revenue AS (
    SELECT
        da.author_name,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM distinct_authors AS da
    LEFT JOIN {{ ref('int_books') }} AS books
        ON TRIM(UPPER(books.author)) = da.author_name  -- Match normalized names
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY da.author_name
)

SELECT *
FROM author_revenue
WHERE author_name IS NOT NULL
