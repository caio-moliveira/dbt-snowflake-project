{% snapshot stg_raw_books_snapshot %}

{{ config(
    target_database='DBT_PROJECT',
    target_schema='snapshots',
    unique_key='unique_id', -- Use the generated unique key
    strategy='timestamp',
    updated_at='updated_at',
    check_cols=['*']
) }}

SELECT
    MD5(CONCAT(sales_year, '-', Position)) AS unique_id,
    *
FROM {{ source('dbt_staging', 'stg_raw_books') }}


{% endsnapshot %}
