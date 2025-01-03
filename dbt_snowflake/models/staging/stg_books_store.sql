WITH raw_data AS (
    SELECT
        source,
        data:Title::STRING AS title,  -- Ensure `data` is a valid JSON column
        data:Author::STRING AS author,
        data:Price::FLOAT AS price
    FROM {{ source('raw', 'books') }}
)
SELECT *
FROM raw_data;
