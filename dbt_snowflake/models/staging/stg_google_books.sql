WITH filtered_data AS (
    SELECT
        PARSE_JSON(data) AS json_data
    FROM {{ source('source', 'books') }}
    WHERE source = 'google_books'
),
flattened_titles AS (
    SELECT 
        KEY AS book_index,
        VALUE::STRING AS title
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:title)
),
flattened_authors AS (
    SELECT 
        KEY AS book_index,
        VALUE::STRING AS author
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:authors)
),
flattened_ratings AS (
    SELECT 
        KEY AS book_index,
        TRY_CAST(VALUE::STRING AS FLOAT) AS average_rating
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:averageRating)
),
flattened_ratings_count AS (
    SELECT 
        KEY AS book_index,
        TRY_CAST(VALUE::STRING AS INTEGER) AS ratings_count
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:ratingsCount)
),
flattened_dates AS (
    SELECT 
        KEY AS book_index,
        VALUE::STRING AS published_date
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:publishedDate)
)
SELECT 
    titles.title,
    authors.author,
    ratings.average_rating,
    ratings_count.ratings_count,
    dates.published_date
FROM 
    flattened_titles AS titles
    LEFT JOIN flattened_authors AS authors ON titles.book_index = authors.book_index
    LEFT JOIN flattened_ratings AS ratings ON titles.book_index = ratings.book_index
    LEFT JOIN flattened_ratings_count AS ratings_count ON titles.book_index = ratings_count.book_index
    LEFT JOIN flattened_dates AS dates ON titles.book_index = dates.book_index