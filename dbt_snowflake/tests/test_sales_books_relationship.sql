-- Test that all ISBNs in stg_sales exist in stg_books
SELECT
    sales.ISBN
FROM {{ ref('stg_sales') }} sales
LEFT JOIN {{ ref('stg_books') }} books
ON sales.ISBN = books.ISBN
WHERE books.ISBN IS NULL
