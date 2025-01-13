{{ config(
    materialized='view',
    unique_key='goodreads_id'
) }}

WITH source AS (
        SELECT DISTINCT
            ISBN,
            GR_Title AS title,
            GR_Author AS author,
            GR_Rating AS rating,
            GR_Pages AS pages,
            Ingestion_Time AS ingestion_time
    FROM {{ source('dbt_staging', 'stg_goodreads') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY ISBN) AS goodreads_id,
            ISBN,
            title,
            author,
            rating,
            pages,
            ingestion_time
        FROM source
    )


SELECT *
FROM deduplicated
