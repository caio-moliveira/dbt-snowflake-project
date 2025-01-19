from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG

# Define constants for S3
S3_BUCKET = "mybooksreviewproject"
S3_PREFIX = "csv/"
AWS_CONN_ID = "aws_default"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Define dbt ProfileConfig
profile_config = ProfileConfig(
    profile_name="dbt_snowflake",
    target_name="dev",
    profiles_yml_filepath=Path("/usr/local/airflow/dbt_snowflake/profiles.yml"),
)

# Define dbt ProjectConfig
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dbt_snowflake", models_relative_path="models"
)

# Define the DAG
with DAG(
    dag_id="s3_to_dbt_pipeline_always_on",
    default_args=default_args,
    description="Continuously monitor S3 for new files and trigger dbt workflows",
    schedule_interval=None,  # No schedule, always triggered manually or self-triggering
    catchup=False,  # Don't backfill
    max_active_runs=1,  # Ensure only one active run at a time
) as dag:
    # Task 1: Wait for a new file in S3
    s3_sensor = S3KeySensor(
        task_id="s3_sensor",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}*",
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,  # Enable wildcard matching to detect any file under the prefix
        poke_interval=30,  # Check every 30 seconds
        mode="poke",  # Occupy a worker slot while poking
        timeout=0,  # No timeout, wait indefinitely for a file
    )

    # Task 2: Run dbt TEST
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {project_config.dbt_project_path} "
            f"--profiles-dir {profile_config.profiles_yml_filepath.parent} "
            f"--target {profile_config.target_name}"
        ),
    )

    # Task 3: Run dbt MODELS
    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        profile_config=profile_config,
        project_config=project_config,
        default_args={"retries": 2},
    )

    # Task 4: Run dbt SNAPSHOT
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"dbt snapshot --project-dir {project_config.dbt_project_path} "
            f"--profiles-dir {profile_config.profiles_yml_filepath.parent} "
            f"--target {profile_config.target_name}"
        ),
    )

    # Define task dependencies
    s3_sensor >> dbt_test >> dbt_running_models >> dbt_snapshot
