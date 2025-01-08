{{ config(
    materialized='view'
) }}

SELECT
    $1 AS ISBN,
    $2::string AS title,
    $3::string AS author,
    $4::float AS rating,
    $5::integer AS pages,
FROM @DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage/goodreads_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FORMATS.MY_CSV_FORMAT')
