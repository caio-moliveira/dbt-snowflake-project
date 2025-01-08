{{ config(
    materialized='view'
) }}

SELECT
    $1 AS ISBN,
    $2::string AS title,
    $3::string AS author,
    $4::string AS imprint,
    $5::string AS publisher_group,
    $6::string AS binding,
    TRY_TO_DATE($7, 'DD/MM/YYYY') AS publication_date,
    $8::string AS product_class
FROM @DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage/books_table.csv
(FILE_FORMAT => 'DBT_PROJECT.FILE_FORMATS.MY_CSV_FORMAT')
