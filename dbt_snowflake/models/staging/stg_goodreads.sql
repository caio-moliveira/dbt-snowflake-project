{{ config(
    materialized='incremental',
    unique_key='goodreads_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'goodreads') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY GR_Author ASC) AS goodreads_id,
            ISBN,
            GR_Title AS title,
            GR_Author AS author,
            GR_Rating AS rating,
            TRY_CAST(GR_Pages AS INTEGER) AS pages,
            current_timestamp() AS dbt_load_date,
        FROM source
    )


SELECT *
FROM deduplicated
{% if is_incremental() %}
WHERE dbt_load_date > (SELECT MAX(dbt_load_date) FROM {{ this }})
{% endif %}
