from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from get_cities.get_cities_controller import get_cities_controller

default_args = {
    'owner': 'rthorski',
    'start_date': datetime.datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
  dag_id="get_cities",
  default_args=default_args,
  schedule=None,
) as dag:
  
  get_cities = PythonOperator(
    task_id='get_cities',
    python_callable=get_cities_controller
  )
