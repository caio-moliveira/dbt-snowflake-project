WITH raw_data AS (
    SELECT
        source,
        data
    FROM {{ ref('models_raw', 'books') }}
    WHERE source = 'google_books'
),
flattened AS (
    SELECT
        source,
        data:title::STRING AS title,
        data:authors[0]::STRING AS author, -- Extracting the first author
        data:description::STRING AS description,
        data:averageRating::FLOAT AS average_rating,
        data:ratingsCount::INT AS ratings_count,
        data:publishedDate::STRING AS published_date,
        data:categories[0]::STRING AS category, -- Extracting the first category
        data:language::STRING AS language
    FROM raw_data
)
SELECT * FROM flattened;
