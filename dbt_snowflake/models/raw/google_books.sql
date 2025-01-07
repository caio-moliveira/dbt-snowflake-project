{{ config(
    materialized='table'
) }}

SELECT
    $1 AS ISBN,
    $2 AS GB_Title,
    $3 AS GB_Author,
    $4 AS GB_Desc,
    $5 AS GB_Pages,
    $6 AS GB_Genre
FROM @DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage/google_books_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FORMATS.MY_CSV_FORMAT')