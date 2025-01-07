{{ config(
    materialized='table'
) }}

SELECT
    $1 AS ISBN,
    $2 AS GR_Title,
    $3 AS GR_Author,
    $4 AS GR_Rating,
    $5 AS GR_Pages
FROM @DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage/goodreads_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FORMATS.MY_CSV_FORMAT')