from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 8),
    "email": ["moliveiracaio@gmail.com"],  # Add email for notifications
    "email_on_failure": True,  # Enable email notifications on failure
    "email_on_retry": True,  # Enable email notifications on retries
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
    catchup=False,
) as dag:
    # Task 1: Wait for a new file in S3
    s3_sensor = S3KeySensor(
        task_id="s3_sensor",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}*",
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,  # Enable wildcard matching to detect any file under the prefix
        poke_interval=30,
        mode="reschedule",  # Check every 30 seconds
    )

    # Define task dependencies
    s3_sensor
