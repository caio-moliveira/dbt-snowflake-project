-- Custom Test: Validate that 'binding' contains only 'Paperback' or 'Hardback'

WITH invalid_binding AS (
  SELECT binding
  FROM {{ ref('int_books') }}
  WHERE binding NOT IN ('Paperback', 'Hardback')
)

SELECT *
FROM invalid_binding
