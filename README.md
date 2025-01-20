# Data Pipeline Project: S3 - SQS - Snowflake - DBT - Airflow - Streamlit

## Introduction

This project implements a data pipeline that integrates AWS S3, Snowflake, DBT, Airflow, and Streamlit for efficient data ingestion, transformation, and analysis. The pipeline automates the process of transferring data from S3 to Snowflake, applying transformations with DBT, and visualizing insights through a Streamlit web application.

---

## Table of Contents

1. [Project Overview](#project-overview)  
2. [Pipeline Workflow](#pipeline-workflow)  
3. [Technologies Used](#technologies-used)  
4. [Installation](#installation)  
5. [Usage](#usage)  
6. [Features](#features)  
7. [Pipeline Components](#pipeline-components)  
8. [Configuration](#configuration)  
9. [Troubleshooting](#troubleshooting)  
10. [Contributors](#contributors)  
11. [License](#license)  

---

## Project Overview

This project automates the end-to-end process of data ingestion, transformation, and visualization:
- **Ingestion**: Files dropped into an S3 bucket trigger an SQS message and Snowflake's `snowpipe` to load raw data into Snowflake.
- **Transformation**: DBT runs transformations to organize data into `Intermediate` and `Marts` layers.
- **Visualization**: A Streamlit web application connects to the Snowflake `Marts` schema for data analysis and displays visual insights.

---

## Pipeline Workflow

1. **S3 File Upload**  
   - Files are uploaded to an AWS S3 bucket.  
   - The upload triggers Snowflake's `snowpipe`, loading the file into the `Raw` schema.

2. **Airflow Sensor**  
   - Airflow's `S3KeySensor` monitors the S3 bucket for new files.  
   - Upon detection, Airflow executes a task to run `dbt run`.

3. **DBT Transformation**  
   - DBT applies transformations, organizing data into `Intermediate` and `Marts` layers in Snowflake.  
   - Data is validated and optimized for downstream use.

4. **Streamlit Application**  
   - A web application built with Streamlit connects to Snowflake's `Marts` schema to visualize and analyze the processed data.

---

## Technologies Used

- **AWS S3**: Cloud storage for raw data files.  
- **Snowflake**: Cloud-based data warehouse.  
- **DBT (Data Build Tool)**: For SQL-based data transformations and schema management.  
- **Apache Airflow**: Workflow orchestration tool for task automation.  
- **Streamlit**: Framework for building interactive web-based dashboards.  

---

## Setting Enviroment

1. Clone the repository:  
   ```bash
   mkdir dbt-snowflake-project
   git clone https://github.com/caio-moliveira/dbt-snowflake-project.git
   cd dbt-snowflake-project 
    ```

1. Set Snowflake Enviroment:  
   ``` sql

-- Switch to the ACCOUNTADMIN role to perform setup
USE ROLE ACCOUNTADMIN;

-- Step 1: Create the dbt_transform role 
CREATE OR REPLACE ROLE dbt_role;
GRANT ROLE dbt_role TO ROLE ACCOUNTADMIN;
```

``` sql 
-- Step 2: Create the warehouse
CREATE WAREHOUSE IF NOT EXISTS DBT_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  MAX_CLUSTER_COUNT = 3
  AUTO_SUSPEND = 300 -- Seconds
  COMMENT = 'This is a warehouse for development and testing';
```
```sql
-- Step 3: Create the dbt_user and assign it to the role
CREATE USER IF NOT EXISTS dbt_user
    PASSWORD = 'your-password' 
    LOGIN_NAME = 'dbt_user'
    MUST_CHANGE_PASSWORD = FALSE
    DEFAULT_WAREHOUSE = 'DBT_WH'
    DEFAULT_ROLE = 'dbt_role'
    COMMENT = 'dbt user for operations and data transformation';

GRANT ROLE dbt_role TO USER dbt_user;
```
```sql 
-- Step 4: Create the database and schemas
CREATE OR REPLACE DATABASE DBT_PROJECT;
CREATE OR REPLACE SCHEMA DBT_PROJECT.DBT_STAGING;
```
```sql 
-- Create a storage integration for S3
CREATE OR REPLACE STORAGE INTEGRATION s3_project_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/')
  COMMENT = 'Integration for S3 bucket with CSV files';

-- Describe the integration to confirm setup - You will need to take an ID to use when Setting the IAM Policies
DESC INTEGRATION s3_project_integration;
```
``` sql
CREATE OR REPLACE FILE FORMAT DBT_PROJECT.FILE_FORMATS.my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'      
NULL_IF = ('NULL', 'N/A')           
FIELD_DELIMITER = ','
PARSE_HEADER = TRUE;

```
``` sql
-- Recreate in the correct schema
CREATE OR REPLACE STAGE DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage
  URL = 's3://your-bucket-name/'
  STORAGE_INTEGRATION = s3_project_integration
  FILE_FORMAT = DBT_PROJECT.FILE_FORMATS.my_csv_format;
```


1. Creating SnowPipe:  
``` sql
CREATE OR REPLACE PIPE DBT_PROJECT.DBT_STAGING.snowpipe_books AUTO_INGEST=TRUE
AS
COPY INTO DBT_PROJECT.DBT_STAGING.stg_raw_books
FROM @DBT_PROJECT.EXTERNAL_STAGES.s3_books_stage
FILE_FORMAT = (FORMAT_NAME = 'DBT_PROJECT.FILE_FORMATS.my_csv_format')
match_by_column_name=case_insensitive;
```

1. Clone the repository:  
   ```bash
    ```
1. Clone the repository:  
   ```bash
    ```
1. Clone the repository:  
   ```bash
    ```
1. Clone the repository:  
   ```bash
    ```
1. Clone the repository:  
   ```bash
    ```
1. Clone the repository:  
   ```bash
    ```


## Usage

1. **File Upload**: Drop a file into the S3 bucket.  
2. **Trigger Workflow**: The S3 upload triggers `snowpipe` and Airflow automation.  
3. **Run Transformations**: DBT organizes and transforms the data into Snowflake schemas.  
4. **Analyze Data**: Access the Streamlit web application to view analysis and insights.

---

## Features

- Automated ingestion of files from S3 to Snowflake.  
- Transformation of data into organized schemas (`Raw`, `Intermediate`, `Marts`).  
- Scheduled workflows using Airflow.  
- Real-time data analysis via Streamlit dashboards.

---

## Pipeline Components

### 1. S3 Bucket and Snowpipe

Snowpipe automatically ingests files uploaded to the S3 bucket and stages them in the `Raw` schema within Snowflake.

### 2. Airflow DAG

Airflow detects new files in the S3 bucket and triggers DBT to process the raw data.

### 3. DBT Models

DBT organizes and transforms data into `Intermediate` and `Marts` schemas in Snowflake.

### 4. Streamlit Application

The Streamlit application fetches data from the `Marts` schema to visualize results and insights.

---

## Configuration

### AWS S3 and Snowflake
- Configure AWS S3 bucket permissions and triggers.  
- Set up Snowflake stages and pipes to automate ingestion.

### Airflow
- Define DAGs and tasks to monitor S3 and execute DBT commands.

### DBT
- Configure `profiles.yml` with Snowflake credentials for transformations.

### Streamlit
- Update Snowflake connection settings in the application for seamless integration.

---

## Troubleshooting

- **S3 Upload Errors**: Ensure the bucket's permissions and triggers are correctly configured.  
- **Snowpipe Issues**: Verify Snowflake stage and pipe setup.  
- **DBT Transformation Errors**: Check SQL models and their dependencies.  
- **Streamlit Connection Problems**: Confirm Snowflake credentials and network access.
