{{ config(
    materialized='view',
    unique_key='google_id'
) }}

WITH source AS (
    SELECT
            ISBN,
            GB_Title AS title,
            GB_Author AS author,
            GB_Desc AS description,
            GB_Pages AS pages,
            GB_Genre AS genre,
            Ingestion_Time AS ingestion_time
    FROM {{ source('dbt_staging', 'stg_google_books') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY author ASC) AS google_id,
            ISBN,
            title,
            author,
            description,
            pages,
            genre,
            ingestion_time
        FROM source
    )

SELECT *
FROM deduplicated
