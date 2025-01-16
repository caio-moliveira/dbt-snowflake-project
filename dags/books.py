from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="s3_to_dbt_pipeline",
    default_args=default_args,
    description="Orchestrate dbt workflows when new files are dropped in S3",
    schedule_interval=None,  # Trigger manually or event-based
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    # Task 1: Wait for a new file in S3
    s3_sensor = S3KeySensor(
        task_id="s3_sensor",
        bucket_name="mybooksreviewproject",
        bucket_key="s3://mybooksreviewproject/csv/*.csv",  # Use a pattern to match files
        aws_conn_id="aws_default",  # Airflow connection for AWS
        timeout=600,  # Timeout after 10 minutes
        poke_interval=30,  # Check every 30 seconds
    )

    # Task 2: Run dbt transformations
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir /dbt-snowflake-project/dbt_snowflake/profiles.yml --project-dir /dbt-snowflake-project/dbt_snowflake/",
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir /dbt-snowflake-project/dbt_snowflake/profiles.yml --project-dir /dbt-snowflake-project/dbt_snowflake/",
    )

    # Task 4: Run dbt snapshots (optional)
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="dbt snapshot --profiles-dir /dbt-snowflake-project/dbt_snowflake/profiles.yml --project-dir /dbt-snowflake-project/dbt_snowflake/",
    )

    # Define task dependencies
    s3_sensor >> dbt_run >> dbt_test >> dbt_snapshot
