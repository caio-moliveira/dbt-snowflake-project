{{ config(
    materialized='view',
    unique_key='google_id'
) }}

WITH source AS (
    SELECT
            ISBN,
            title,
            author,
            description,
            pages,
            genre
    FROM {{ ref('stg_google_books') }}
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
            current_timestamp() AS dbt_load_date
        FROM source
    )

SELECT *
FROM deduplicated
