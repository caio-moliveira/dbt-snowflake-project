{{ config(
    materialized='incremental',
    unique_key='ISBN'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'google_books') }}
),

cleaned AS (
    SELECT DISTINCT
        ISBN,
        GB_Title AS Title,
        GB_Author AS Author,
        GB_Desc AS Description,
        GB_Pages::INTEGER AS Pages,
        GB_Genre AS Genre,
        current_timestamp() AS dbt_load_date
    FROM source
    WHERE ISBN IS NOT NULL
)

SELECT *
FROM cleaned
{% if is_incremental() %}
WHERE dbt_load_date > (SELECT MAX(dbt_load_date) FROM {{ this }})
{% endif %}
