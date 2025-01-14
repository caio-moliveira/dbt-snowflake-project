{{ config(
    materialized='view',
    unique_key='ISBN'
) }}


WITH deduplicated_books AS (
    SELECT DISTINCT
        ISBN,
        Title AS book_title,
        Author AS book_author,
        Volume AS volume_sold,
        Binding AS binding,
        Publ_Date AS publish_date,
        Product_Class AS product_class
    FROM {{ source('dbt_staging', 'stg_raw_books') }}
    WHERE ISBN IS NOT NULL
)

SELECT *
FROM deduplicated_books
ORDER BY ISBN
