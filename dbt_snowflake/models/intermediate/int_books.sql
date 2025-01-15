{{ config(
    materialized='view',
    unique_key='ISBN'
) }}


WITH deduplicated_books AS (
    SELECT DISTINCT
        ISBN,
        Title AS book_title,
        Author AS book_author,
        Binding AS binding,
        Publ_Date AS publish_date,
        Product_Class AS product_class
    FROM {{ source('dbt_staging', 'stg_raw_books') }}
)

SELECT *
FROM deduplicated_books
WHERE book_author IS NOT NULL
ORDER BY ISBN
