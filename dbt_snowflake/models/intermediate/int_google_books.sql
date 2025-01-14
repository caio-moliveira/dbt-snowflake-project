{{ config(
    materialized='view',
    unique_key='ISBN'
) }}

WITH deduplicated_google_books AS (
    SELECT DISTINCT
        ISBN,
        GB_Title AS gb_title,
        GB_Author AS gb_author,
        GB_Desc AS gb_desc,
        GB_Pages AS gb_pages,
        GB_Genre AS gb_genre
    FROM {{ source('dbt_staging', 'stg_raw_books') }}
    WHERE ISBN IS NOT NULL
)

SELECT *
FROM deduplicated_google_books
ORDER BY ISBN
