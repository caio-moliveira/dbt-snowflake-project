WITH raw_data AS (
    SELECT
        source,
        data
    FROM {{ ref('RAW.books') }}
    WHERE source = 'bookstore'
),
flattened AS (
    SELECT
        source,
        data:Title::STRING AS title,
        data:Star Rating::STRING AS star_rating,
        data:Price::FLOAT AS price
    FROM raw_data
)
SELECT * FROM flattened;
