SELECT *
FROM {{ ref('stg_books') }}
WHERE publication_date IS NULL

