-- Test that titles in Amazon exist in at least one other source
SELECT amazon.title
FROM {{ ref('stg_amazon_books') }} amazon
LEFT JOIN (
    SELECT title FROM {{ ref('stg_google_books') }}
    UNION
    SELECT title FROM {{ ref('stg_mercadolivre_books') }}
    UNION
    SELECT title FROM {{ ref('stg_books_store') }}
) other_sources
ON amazon.title = other_sources.title
WHERE other_sources.title IS NULL;
