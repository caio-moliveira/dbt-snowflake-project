{{ config(
    materialized='view',
    unique_key='ISBN'
) }}


SELECT DISTINCT
        ISBN,
        TRIM(title) AS title,
        TRIM(author) AS author,
        TRIM(imprint) AS imprint,
        publisher_group,
        binding,
        publication_date,
        product_class
FROM {{ ref('stg_books') }}
WHERE ISBN IS NOT NULL
