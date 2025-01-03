WITH amazon_books AS (
    SELECT
        title,
        rank,
        reviews AS average_rating,
        review_count,
        price,
        genre AS category,
        manufacturer AS publisher,
        author,
        brand,
        number_of_pages
    FROM {{ ref('amazon_books') }}
)
SELECT * FROM {{ ref('amazon_books') }}

