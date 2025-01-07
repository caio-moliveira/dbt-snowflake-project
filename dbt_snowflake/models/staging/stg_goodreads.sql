{{ config(
    materialized='incremental',
    unique_key='ISBN'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'goodreads') }}
),

deduplicated AS (
    SELECT DISTINCT
        ISBN,
        GR_Title,
        GR_Author,
        GR_Rating,
        GR_Pages,
        current_timestamp() AS dbt_load_date
    FROM source
    WHERE ISBN IS NOT NULL
)

SELECT *
FROM deduplicated
{% if is_incremental() %}
WHERE dbt_load_date > (SELECT MAX(dbt_load_date) FROM {{ this }})
{% endif %}
