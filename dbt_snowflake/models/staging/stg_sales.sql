{{ config(
    materialized='incremental',
    unique_key='sales_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'sales') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY Sales_Year DESC, Position ASC) AS sales_id,
            ISBN,
            Sales_Year::INTEGER AS sales_year,
            Position::INTEGER AS rank_position,
            Volume::FLOAT AS sales_volume,
            Value::FLOAT AS sales_value,
            RRP::FLOAT AS recommended_retail_price,
            ASP::FLOAT AS average_sales_price,
            current_timestamp() AS dbt_load_date,
        FROM source
    )

SELECT *
FROM deduplicated
{% if is_incremental() %}
WHERE dbt_load_date > (SELECT MAX(dbt_load_date) FROM {{ this }})
{% endif %}
