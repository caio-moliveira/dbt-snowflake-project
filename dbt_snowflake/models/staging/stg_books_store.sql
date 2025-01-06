 WITH parsed_data AS (
     SELECT
         PARSE_JSON(data) AS json_data
     FROM {{ source('source', 'books') }}
     WHERE source = 'bookstore'
 ),
 flattened_titles AS (
     SELECT
         KEY AS book_index,
         VALUE::STRING AS title
     FROM parsed_data,
     LATERAL FLATTEN(INPUT => json_data:Title)
 ),
 flattened_ratings AS (
     SELECT
         KEY AS book_index,
         VALUE::STRING AS star_rating
     FROM parsed_data,
     LATERAL FLATTEN(INPUT => json_data:"Star Rating")
 ),
 flattened_prices AS (
     SELECT
         KEY AS book_index,
         VALUE::FLOAT AS price
     FROM parsed_data,
     LATERAL FLATTEN(INPUT => json_data:Price)
 )
 SELECT
     titles.title,
     ratings.star_rating,
     prices.price
 FROM
     flattened_titles AS titles
     JOIN flattened_ratings AS ratings ON titles.book_index = ratings.book_index
     JOIN flattened_prices AS prices ON titles.book_index = prices.book_index