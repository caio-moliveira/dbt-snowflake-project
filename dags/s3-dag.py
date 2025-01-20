from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# Define your S3 bucket and prefix
S3_BUCKET = "mybooksreviewproject"
S3_PREFIX = "*.csv"

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
dag = DAG(
    dag_id="s3-sensor",
    default_args=default_args,
    description="Wait for any .csv file in S3 and trigger the dbt workflow DAG",
    schedule_interval=None,  # Triggered manually or externally
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["s3", "dbt", "data-pipeline"],
)

start = DummyOperator(
    dag=dag,
    task_id="start",
)

# Task 1: S3KeySensor to wait for any .csv file in the S3 folder
s3_sensor = S3KeySensor(
    task_id="s3-sensor",
    bucket_name=S3_BUCKET,  # Your S3 bucket name
    bucket_key=S3_PREFIX,  # Wildcard to match any .csv file
    aws_conn_id="aws_default",  # AWS connection set up in Airflow
    wildcard_match=True,
    poke_interval=30,  # Check every 30 seconds
    timeout=60 * 60 * 6,  # Timeout after 6 hours
    dag=dag,
)

# Task 2: Trigger the dbt DAG
trigger_dbt_dag = TriggerDagRunOperator(
    task_id="trigger_dbt_dag",
    trigger_dag_id="cosmos_dbt_dag",  # The DAG ID of your dbt DAG
    wait_for_completion=False,  # Set to True if you want this DAG to wait for the dbt DAG to finish
    dag=dag,
)

end = DummyOperator(
    dag=dag,
    task_id="end",
)

# Task dependencies
start >> s3_sensor >> trigger_dbt_dag >> end
