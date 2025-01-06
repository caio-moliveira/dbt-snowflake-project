{{ config(
    materialized='table'
) }}

SELECT
    $1 AS ISBN,
    $2 AS Title,
    $3 AS Author,
    $4 AS Imprint,
    $5 AS Publisher_Group,
    $6 AS Binding,
    $7 AS Publ_Date,
    $8 AS Product_Class
FROM @DBT_PROJECT.EXTERNAL_STAGES.MY_S3_STAGE/books_table.csv
(FILE_FORMAT => 'MY_CSV_FORMAT')