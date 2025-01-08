{{ config(
    materialized='view'
) }}

SELECT
    $1 AS ISBN,
    $2::NUMBER(4,0) AS sales_year,
    $3::NUMBER(10,0) AS rank_position,
    $4::FLOAT AS sales_volume,
    $5::FLOAT AS sales_value,
    $6::FLOAT AS recommended_retail_price,
    $7::FLOAT AS average_sales_price
FROM @DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage/sales_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FORMATS.MY_CSV_FORMAT')
