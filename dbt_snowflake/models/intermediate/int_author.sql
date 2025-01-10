{{ config(
    materialized='view',
    unique_key='author_id'
) }}


WITH distinct_authors AS (
    SELECT DISTINCT
        TRIM(UPPER(author)) AS author_name,
        publisher_group
    FROM {{ ref('stg_books') }}
    WHERE author IS NOT NULL
),
indexed_authors AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY author_name) AS author_id,
        author_name,
        publisher_group
    FROM distinct_authors
)
SELECT *
FROM indexed_authors;
