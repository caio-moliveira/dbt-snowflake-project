from airflow.operators.bash import BashOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


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


# Task 3: Cosmos-generated dbt tasks
cosmos_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        "/usr/local/airflow/dbt_snowflake",
    ),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    schedule_interval=None,
    catchup=False,
    dag_id="cosmos_dbt_dag",
)
# Task 2: Verify dbt setup with `dbt debug`
dbt_debug = BashOperator(
    task_id="dbt_debug",
    bash_command="dbt debug --profiles-dir /usr/local/airflow/dbt_snowflake --project-dir /usr/local/airflow/dbt_snowflake",
)

# Task 4: Run dbt snapshots
dbt_snapshot = BashOperator(
    task_id="dbt_snapshot",
    bash_command="dbt snapshot --profiles-dir /usr/local/airflow/dbt_snowflake --project-dir /usr/local/airflow/dbt_snowflake",
)
