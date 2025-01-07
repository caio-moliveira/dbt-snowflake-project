{{ config(
    materialized='incremental',
    unique_key='ISBN'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'sales') }}
),

deduplicated AS (
    SELECT DISTINCT
        ISBN,
        Sales_Year::INTEGER AS Year,
        Position::INTEGER AS Rank_Position,
        Volume::FLOAT AS Sales_Volume,
        Value::FLOAT AS Sales_Value,
        RRP::FLOAT AS Recommended_Retail_Price,
        ASP::FLOAT AS Average_Selling_Price,
        current_timestamp() AS dbt_load_date
    FROM source
    WHERE ISBN IS NOT NULL
),

validated AS (
    SELECT *
    FROM deduplicated
    WHERE Year BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE)
      AND Sales_Volume >= 0
      AND Sales_Value >= 0
)

SELECT *
FROM validated
{% if is_incremental() %}
WHERE Year > (SELECT MAX(Year) FROM {{ this }})
{% endif %}
