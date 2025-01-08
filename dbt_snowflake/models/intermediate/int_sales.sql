{{ config(
    materialized='view',
    unique_key='sales_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_sales') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY sales_year DESC, rank_position ASC) AS sales_id,
            ISBN,
            sales_year,
            rank_position,
            sales_volume,
            sales_value,
            recommended_retail_price,
            average_sales_price,
            current_timestamp() AS dbt_load_date
        FROM source
    )

SELECT *
FROM deduplicated
