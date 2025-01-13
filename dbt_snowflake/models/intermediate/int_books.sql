{{ config(
    materialized='view',
    unique_key='ISBN'
) }}


SELECT DISTINCT
        ISBN,
        Title AS title,
        Author AS author,
        Imprint AS imprint,
        Publisher_Group AS publisher_group,
        Binding AS binding,
        Publ_Date AS publication_date,
        Product_Class AS product_class
FROM {{ source('dbt_staging', 'stg_books') }}
WHERE ISBN IS NOT NULL
