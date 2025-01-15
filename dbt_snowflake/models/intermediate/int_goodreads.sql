{{ config(
    materialized='view',
    unique_key='ISBN'
) }}

WITH deduplicated_goodreads AS (
    SELECT DISTINCT
        ISBN,
        Sales_Year as sales_year,
        GR_Title AS gr_title,
        GR_Author AS gr_author,
        GR_Rating AS gr_rating,
        GR_Pages AS gr_pages
    FROM {{ source('dbt_staging', 'stg_raw_books') }}
    WHERE ISBN IS NOT NULL
)

SELECT *
FROM deduplicated_goodreads
ORDER BY ISBN
