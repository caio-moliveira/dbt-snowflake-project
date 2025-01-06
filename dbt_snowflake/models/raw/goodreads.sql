{{ config(
    materialized='table'
) }}

SELECT
    $1 AS ISBN,
    $2 AS GR_Title,
    $3 AS GR_Author,
    $4 AS GR_Rating,
    $5 AS GR_Pages
FROM @DBT_PROJECT.EXTERNAL_STAGES.MY_S3_STAGE/goodreads_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FOMATSR.MY_CSV_FORMAT')