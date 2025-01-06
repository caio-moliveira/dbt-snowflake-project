WITH filtered_data AS (
    SELECT
        PARSE_JSON(data) AS json_data
    FROM {{ source('source', 'books') }}
    WHERE source = 'mercado_livre_books'
),
flattened_titles AS (
    SELECT 
        KEY AS book_index,
        VALUE::STRING AS title
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:title)
),
flattened_prices AS (
    SELECT 
        KEY AS book_index,
        TRY_CAST(VALUE::STRING AS FLOAT) AS price
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:price)
),
flattened_authors AS (
    SELECT 
        KEY AS book_index,
        VALUE::STRING AS author
    FROM filtered_data,
    LATERAL FLATTEN(INPUT => json_data:author)
)
SELECT 
    titles.title,
    prices.price,
    authors.author
FROM 
    flattened_titles AS titles
    LEFT JOIN flattened_prices AS prices ON titles.book_index = prices.book_index
    LEFT JOIN flattened_authors AS authors ON titles.book_index = authors.book_index