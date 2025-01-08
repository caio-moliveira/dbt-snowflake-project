{{ config(
    materialized='view',
    unique_key='author_id'
) }}


WITH authors AS (
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY author) AS author_id,
        author AS author_name,
        publisher_group
    FROM {{ ref('stg_books') }}
    WHERE author IS NOT NULL
)
SELECT *
FROM authors
