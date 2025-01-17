from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path
from datetime import datetime, timedelta

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 8),
    "email": ["moliveiracaio@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Define the S3 bucket and folder details
S3_BUCKET = "mybooksreviewproject"
S3_PREFIX = "csv/"  # Folder path inside the bucket
AWS_CONN_ID = "aws_default"  # Airflow connection for AWS

# Path to the dbt project
dbt_project_path = Path("/usr/local/airflow/dbt_snowflake")

# Configure the ProfileConfig for dbt
profile_config = ProfileConfig(
    profile_name="dbt_snowflake",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="Snowflake",
        profile_args={
            "database": "dbt_project",
            "schema": "dbt",
            "warehouse": "dbt_wh",
        },
    ),
)

# Define the main DAG
with DAG(
    dag_id="s3_to_dbt_pipeline_multi_file",
    default_args=default_args,
    description="Orchestrate dbt workflows when new files are dropped in S3 and process multiple files",
    schedule_interval=None,
    catchup=False,
) as dag:
    # Task 1: Wait for a new file in S3
    s3_sensor = S3KeySensor(
        task_id="s3_sensor",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}*",
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,
        poke_interval=30,
        mode="reschedule",
    )

    # Task 2: Verify dbt setup with `dbt debug`
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="dbt debug --profiles-dir /usr/local/airflow/dbt_snowflake --project-dir /usr/local/airflow/dbt_snowflake",
    )

    # Task 3: Cosmos-generated dbt tasks
    cosmos_dbt_dag = DbtDag(
        dag_id="cosmos_dbt_dag",  # Provide a unique DAG ID
        project_config=ProjectConfig(dbt_project_path),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/bin/dbt",
        ),
    )

    # Task 4: Run dbt snapshots
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="dbt snapshot --profiles-dir /usr/local/airflow/dbt_snowflake --project-dir /usr/local/airflow/dbt_snowflake",
    )

    # Define task dependencies
    s3_sensor >> dbt_debug >> cosmos_dbt_dag >> dbt_snapshot
