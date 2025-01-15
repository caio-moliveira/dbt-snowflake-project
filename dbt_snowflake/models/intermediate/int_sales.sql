{{ config(
    materialized='view',
    unique_key='sales_id'
) }}

WITH deduplicated_sales AS (
    SELECT DISTINCT
        ISBN,
        Sales_Year AS sales_year,
        Position AS position,
        Volume AS volume_sold,
        Value AS sales_value,
        RRP AS recommended_retail_price,
        ASP AS average_selling_price
    FROM {{ source('dbt_staging', 'stg_raw_books') }}
    WHERE ISBN IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY ISBN, sales_year, position) AS sales_id,
    ISBN,
    sales_year,
    position,
    volume_sold,
    sales_value,
    recommended_retail_price,
    average_selling_price
FROM deduplicated_sales
ORDER BY ISBN, sales_year
