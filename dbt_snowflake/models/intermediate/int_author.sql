{{ config(
    materialized='view',
    unique_key='author_id'
) }}


WITH deduplicated_authors AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY COALESCE(Author, 'UNKNOWN')) AS author_id,
        Author AS author_name,
        Publisher_Group AS publisher_group,
        AVG(GR_Rating) AS avg_rating
    FROM (
        SELECT DISTINCT
            Author,
            Publisher_Group,
            GR_Rating
        FROM {{ source('dbt_staging', 'stg_raw_books') }}
        WHERE Author IS NOT NULL
    ) AS unique_authors
    GROUP BY Author, Publisher_Group
)

SELECT
    author_id,
    author_name,
    publisher_group,
    ROUND(avg_rating, 2) AS avg_rating
FROM deduplicated_authors
ORDER BY author_id
