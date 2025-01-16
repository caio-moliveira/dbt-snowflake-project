from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# Define the S3 bucket and folder details
S3_BUCKET = "mybooksreviewproject"
S3_PREFIX = "csv/"  # Folder path inside the bucket
AWS_CONN_ID = "aws_default"  # Airflow connection for AWS

# DAG Definition
with DAG(
    dag_id="s3_to_dbt_pipeline_multi_file",
    default_args=default_args,
    description="Orchestrate dbt workflows when new files are dropped in S3 and process multiple files",
    schedule_interval=None,  # Trigger manually or event-based
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    # Task 1: Wait for a new file in S3
    s3_sensor = S3KeySensor(
        task_id="s3_sensor",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}*",
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,  # Enable wildcard matching to detect any file under the prefix
        timeout=600,  # Timeout after 10 minutes
        poke_interval=30,  # Check every 30 seconds
    )

    # Task 3: Run dbt transformations
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir /usr/local/airflow/dbt_snowflake --project-dir /usr/local/airflow/dbt_snowflake",
    )

    # Task 4: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir /usr/local/airflow/dbt_snowflake --project-dir /usr/local/airflow/dbt_snowflake",
    )

    # Task 5: Run dbt snapshots
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="dbt snapshot --profiles-dir /usr/local/airflow/dbt_snowflake --project-dir /usr/local/airflow/dbt_snowflake",
    )

    # Define task dependencies
    s3_sensor >> dbt_run >> dbt_test >> dbt_snapshot
