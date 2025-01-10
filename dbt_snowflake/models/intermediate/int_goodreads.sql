{{ config(
    materialized='view',
    unique_key='goodreads_id'
) }}

WITH source AS (
        SELECT DISTINCT
            ISBN,
            title,
            author,
            rating,
            pages
    FROM {{ ref('stg_goodreads') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY ISBN) AS goodreads_id,
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
