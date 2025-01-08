{{ config(
    materialized='view',
    unique_key='ISBN'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_books') }}
),

duplicates AS (
    SELECT
        ISBN,
        title,
        author,
        imprint,
        publisher_group,
        binding,
        publication_date,
        product_class,
        ROW_NUMBER() OVER (PARTITION BY ISBN, title, author, imprint, publisher_group, binding, publication_date, product_class ORDER BY ISBN) AS rn
    FROM source
)
SELECT *
FROM duplicates
WHERE rn > 1
