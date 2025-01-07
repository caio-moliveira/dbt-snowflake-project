{% macro parse_date(date_column, format) %}
    TRY_TO_DATE({{ date_column }}, '{{ format }}')
{% endmacro %}
