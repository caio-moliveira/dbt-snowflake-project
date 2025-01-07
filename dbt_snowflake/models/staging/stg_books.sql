{{ config(
    materialized='incremental',
    unique_key='books_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_stage', 'books') }}
),

deduplicated AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY Publ_Date ASC) AS books_id,
            ISBN,
            Title AS title,
            Author AS author,
            Imprint AS imprint,
            Publisher_Group AS publisher_group,
            Binding AS binding,
            {{ parse_date('Publ_Date', 'DD/MM/YYYY') }} AS publication_date,
            Product_Class AS product_class,
            current_timestamp() AS dbt_load_date,
            ROW_NUMBER() OVER (PARTITION BY ISBN ORDER BY author DESC) AS row_number
        FROM source
    )

SELECT *
FROM deduplicated
{% if is_incremental() %}
WHERE dbt_load_date > (SELECT MAX(dbt_load_date) FROM {{ this }})
{% endif %}