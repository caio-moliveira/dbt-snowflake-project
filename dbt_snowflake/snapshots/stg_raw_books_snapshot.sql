{% snapshot stg_raw_books_snapshot %}

    {{
        config(
            target_schema='snapshots',
            unique_key='unique_id',
            strategy='check',
            check_cols=['*']
        )
    }}

        SELECT
            HASH(sales_year || '-' || Position) AS unique_id, -- Use Snowflake-compatible hash function
            *
        FROM {{ source('dbt_staging', 'stg_raw_books') }}

{% endsnapshot %}
