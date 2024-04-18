from airflow import DAG
import datetime
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'rthorski',
    'start_date': datetime.datetime(2024, 1, 1),
}

with DAG(
  dag_id="dbt_jobs",
  default_args=default_args,
  schedule=None,
  catchup=False
) as dag:
  
  task_dbt = BashOperator(
    task_id="dbt_run",
    bash_command="cd ${AIRFLOW_HOME}/dags/_dbt_weather && dbt run",
)
  