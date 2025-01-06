WITH raw_data AS (
    SELECT
        source,
        data
    FROM {{ ref('models_raw', 'books') }}
    WHERE source = 'mercado_livre'
),
flattened AS (
    SELECT
        source,
        data:title::STRING AS title,
        data:author::STRING AS author,
        data:description::STRING AS description,
        data:published_date::STRING AS published_date,
        data:price::FLOAT AS price,
        data:publisher::STRING AS publisher,
        data:language::STRING AS language
    FROM raw_data
)
SELECT * FROM flattened;
