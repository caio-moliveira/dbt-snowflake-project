{{ config(
    materialized='view',
    unique_key='goodreads_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_goodreads') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY ISBN ORDER BY author ASC) AS goodreads_id,
            ISBN,
            title,
            author,
            rating,
            pages,
            current_timestamp() AS dbt_load_date
        FROM source
    )


SELECT *
FROM deduplicated
