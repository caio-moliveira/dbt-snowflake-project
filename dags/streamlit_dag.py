from airflow import DAG
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

with DAG(
    dag_id="run_streamlit",
    default_args=default_args,
    description="Run Streamlit app from Airflow include folder",
    schedule_interval=None,
    catchup=False,
) as dag:
    run_streamlit = BashOperator(
        task_id="start_streamlit",
        bash_command=(
            "cd /opt/airflow/include/streamlit_app && "
            "pip install -r streamlit_requirements.txt && "
            "streamlit run streamlit_app.py --server.port=8501 --server.headless=true",
        ),
    )
