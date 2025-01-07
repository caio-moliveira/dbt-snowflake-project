{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    ISBN,
    title,
    author,
    publication_date,
    product_class,
    publisher_group,
    binding
FROM {{ ref('stg_books') }}
