from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path
from datetime import datetime, timedelta

# Use environment variable for dbt project path
dbt_project_path = Path("/usr/local/airflow/dbt_snowflake")

# Default args for the DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

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

# Define the DAG
dag = DAG(
    dag_id="cosmos_dbt_dag",
    default_args=default_args,
    description="Full dbt project workflow",
    schedule_interval=None,  # Triggered manually or by another DAG
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt", "data-pipeline"],
)

# Define Cosmos DbtDag
cosmos_dbt_dag = DbtDag(
    dag_id="cosmos_dbt_dag",
    project_config=ProjectConfig(dbt_project_path),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
)
