{%- macro snowflake__deduplicate(relation, partition_by, order_by) -%}

    select *
    from {{ relation }}
    qualify
        row_number() over (
            partition by {{ partition_by }}
            order by {{ order_by }}
        ) = 1

{%- endmacro -%}
