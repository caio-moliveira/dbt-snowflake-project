WITH parsed_data AS (
    SELECT 
        PARSE_JSON(data) AS json_data
    FROM {{ source('source', 'books') }}
    WHERE source = 'mercado_livre'
),
flattened_titles AS (
    SELECT 
        KEY AS book_index,
        VALUE::STRING AS title
    FROM parsed_data,
    LATERAL FLATTEN(INPUT => json_data:title)
),
flattened_reviews AS (
    SELECT 
        KEY AS book_index,
        VALUE::FLOAT AS review
    FROM parsed_data,
    LATERAL FLATTEN(INPUT => json_data:review)
),
flattened_prices AS (
    SELECT 
        KEY AS book_index,
        REPLACE(REPLACE(VALUE::STRING, 'R$', ''), ',', '.')::FLOAT AS price
    FROM parsed_data,
    LATERAL FLATTEN(INPUT => json_data:price)
)
SELECT 
    titles.title,
    reviews.review,
    prices.price
FROM 
    flattened_titles AS titles
    LEFT JOIN flattened_reviews AS reviews ON titles.book_index = reviews.book_index
    LEFT JOIN flattened_prices AS prices ON titles.book_index = prices.book_index