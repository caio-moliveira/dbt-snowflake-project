from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define your S3 bucket and prefix
S3_BUCKET = "mybooksreviewproject"
S3_PREFIX = "csv/"
S3_WILDCARD_KEY = f"{S3_PREFIX}*.csv"  # Match any .csv file in the 'csv/' folder

# Default args for the DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="s3_to_dbt_workflow_any_csv",
    default_args=default_args,
    description="Wait for any .csv file in S3 and trigger a full dbt project workflow",
    schedule_interval=None,  # Triggered manually or externally
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["s3", "dbt", "data-pipeline"],
) as dag:
    # Task 1: S3KeySensor to wait for any .csv file in the S3 folder
    wait_for_csv_file = S3KeySensor(
        task_id="wait_for_csv_file",
        bucket_name=S3_BUCKET,  # Your S3 bucket name
        bucket_key=S3_WILDCARD_KEY,  # Wildcard to match any .csv file
        aws_conn_id="aws_default",  # AWS connection set up in Airflow
        poke_interval=60,  # Check every 60 seconds
        timeout=60 * 60 * 6,  # Timeout after 6 hours
        mode="poke",
        verify=True,
    )

    # Task 2: Run dbt debug
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="cd /usr/local/airflow/dbt_snowflake && dbt debug",  # Update path to your dbt project
        env={
            "DBT_PROFILES_DIR": "/usr/local/airflow/dbt_snowflake/profiles.yml",  # Path to dbt profiles directory
        },
    )

    # Task 3: Run dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /usr/local/airflow/dbt_snowflake && dbt test --select state:modified+",
        env={
            "DBT_PROFILES_DIR": "/usr/local/airflow/dbt_snowflake/profiles.yml",
        },
    )

    # Task 4: Run dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /usr/local/airflow/dbt_snowflake && dbt run",
        env={
            "DBT_PROFILES_DIR": "/usr/local/airflow/dbt_snowflake/profiles.yml",
        },
    )

    # Task 5: Run dbt snapshot
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /usr/local/airflow/dbt_snowflake && dbt snapshot",
        env={
            "DBT_PROFILES_DIR": "/usr/local/airflow/dbt_snowflake/profiles.yml",
        },
    )

    # Task dependencies
    wait_for_csv_file >> dbt_debug >> dbt_test >> dbt_run >> dbt_snapshot
