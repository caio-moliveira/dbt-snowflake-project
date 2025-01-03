WITH amazon_books AS (
    SELECT
        title,
        author,
        rank,
        reviews AS average_rating,
        review_count,
        price,
        genre AS category,
        manufacturer AS publisher,
        number_of_pages
    FROM {{ ref('seed') }}
)
SELECT * FROM amazon_books;
