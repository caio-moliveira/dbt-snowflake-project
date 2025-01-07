{{ config(
    materialized='incremental',
    unique_key='ISBN'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'books') }}
),

deduplicated AS (
    SELECT DISTINCT
        ISBN,
        Title,
        Author,
        Imprint,
        Publisher_Group,
        Binding,
        {{ parse_date('Publ_Date', 'DD/MM/YYYY') }} AS Publication_Date, -- Use the macro here
        Product_Class,
        current_timestamp() AS dbt_load_date
    FROM source
    WHERE ISBN IS NOT NULL
)

SELECT *
FROM deduplicated
{% if is_incremental() %}
WHERE dbt_load_date > (SELECT MAX(dbt_load_date) FROM {{ this }})
{% endif %}
