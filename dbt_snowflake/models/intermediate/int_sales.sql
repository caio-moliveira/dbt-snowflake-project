{{ config(
    materialized='view',
    unique_key='sales_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('dbt_staging', 'stg_sales') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY Sales_Year DESC, Position ASC) AS sales_id,
            ISBN,
            Sales_Year AS sales_year,
            Position AS rank_position,
            Volume AS sales_volume,
            Value AS sales_value,
            RRP AS recommended_retail_price,
            ASP AS average_sales_price,
            Ingestion_Time AS ingestion_time
        FROM source
    )

SELECT *
FROM deduplicated
