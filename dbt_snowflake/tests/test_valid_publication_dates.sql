SELECT *
FROM {{ ref('stg_books') }}
WHERE Publication_Date IS NULL
  AND Publ_Date IS NOT NULL; -- Original raw date should not be null
