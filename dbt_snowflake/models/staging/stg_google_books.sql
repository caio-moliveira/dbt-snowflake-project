{{ config(
    materialized='incremental',
    unique_key='google_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'google_books') }}
),

deduplicated AS (
        SELECT
        ROW_NUMBER() OVER (ORDER BY GB_Author ASC) AS google_id,
        ISBN,
        GB_Title AS title,
        GB_Author AS author,
        GB_Desc AS bookdescription,
        GB_Pages::INTEGER AS pages,
        GB_Genre AS genre,
        current_timestamp() AS dbt_load_date,
        FROM source
    )

SELECT *
FROM deduplicated
{% if is_incremental() %}
WHERE dbt_load_date > (SELECT MAX(dbt_load_date) FROM {{ this }})
{% endif %}
