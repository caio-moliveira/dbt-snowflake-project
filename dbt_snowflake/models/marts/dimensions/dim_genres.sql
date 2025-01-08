{{ config(
    materialized='table'
) }}

WITH genres_data AS (
    SELECT
        google.genre AS genre,
        COUNT(DISTINCT books.ISBN) AS books_in_genre,
        COALESCE(SUM(sales.sales_volume), 0) AS total_books_sold,
        COALESCE(SUM(sales.sales_value), 0) AS total_revenue,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('int_google_books') }} AS google
    LEFT JOIN {{ ref('int_books') }} AS books
        ON google.ISBN = books.ISBN
    LEFT JOIN {{ ref('int_sales') }} AS sales
        ON books.ISBN = sales.ISBN
    GROUP BY google.genre
)

SELECT *
FROM genres_data
