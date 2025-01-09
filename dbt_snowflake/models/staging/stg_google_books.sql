{{ config(
    materialized='view'
) }}

SELECT
    $1 AS ISBN,
    $2::string AS title,
    $3::string AS author,
    $4::string AS description,
    $5::int AS pages,
    $6::string AS genre
FROM @DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage/google_books_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FORMATS.MY_CSV_FORMAT')
