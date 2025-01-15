-- Ensure Sales_Year is within a valid range
WITH invalid_years AS (
  SELECT sales_year
  FROM {{ ref('int_sales') }}
  WHERE Sales_Year NOT BETWEEN 2000 AND 2100
)
SELECT *
FROM invalid_years;
