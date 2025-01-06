{{ config(
    materialized='table'
) }}

SELECT
    $1 AS ISBN,
    $2 AS Sales_Year,
    $3 AS Position,
    $4 AS Volume,
    $5 AS Value,
    $6 AS RRP,
    $7 AS ASP
FROM @DBT_PROJECT.EXTERNAL_STAGES.MY_S3_STAGE/sales_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FORMATS.MY_CSV_FORMAT')