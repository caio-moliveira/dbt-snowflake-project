WITH mercadolivre AS (
    SELECT
        'mercado_livre' AS source,
        data:Title::STRING AS title,
        data:Author::STRING AS author,
        data:Description::STRING AS description,
        data:PublishedDate::STRING AS published_date,
        data:Price::FLOAT AS price,
        data:Publisher::STRING AS publisher,
        data:Language::STRING AS language
    FROM DBT_PROJECT.RAW.BOOKS
    WHERE source = 'mercado_livre'
),
google_books AS (
    SELECT
        'google_books' AS source,
        data:Title::STRING AS title,
        data:Authors[0]::STRING AS author, -- First author in the array
        data:Description::STRING AS description,
        data:AverageRating::FLOAT AS average_rating,
        data:RatingsCount::INT AS ratings_count,
        data:PublishedDate::STRING AS published_date,
        data:Categories[0]::STRING AS category, -- First category in the array
        data:Language::STRING AS language
    FROM DBT_PROJECT.RAW.BOOKS
    WHERE source = 'google_books'
),
books AS (
    SELECT
        'bookstore' AS source,
        data:Title::STRING AS title,
        NULL AS author,
        NULL AS description,
        NULL AS published_date,
        data:Price::FLOAT AS price,
        NULL AS publisher,
        NULL AS language,
        data:StarRating::STRING AS star_rating
    FROM DBT_PROJECT.RAW.BOOKS
    WHERE source = 'bookstore'
),
amazon_books AS (
    SELECT
        'amazon' AS source,
        title,
        author,
        NULL AS description,
        NULL AS published_date,
        price,
        publisher,
        NULL AS language,
        average_rating,
        review_count,
        category,
        NULL AS star_rating,
        rank,
        number_of_pages
    FROM {{ ref('stg_amazon_books') }}
),
combined AS (
    SELECT * FROM mercadolivre
    UNION ALL
    SELECT * FROM google_books
    UNION ALL
    SELECT * FROM books
    UNION ALL
    SELECT * FROM amazon_books
)
SELECT * FROM combined;
