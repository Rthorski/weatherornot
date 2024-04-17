from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from weather_or_not.weather_or_not_controller import get_all_informations_meteo, upload_on_gcp, download_from_gcp, load_to_database


default_args = {
    'owner': 'rthorski',
    'start_date': datetime.datetime(2024, 1, 1),
}

with DAG(
  dag_id="weather_or_not",
  default_args=default_args,
  schedule="@hourly",
  catchup=False
) as dag:
  
    task_get_all_informations_meteo = PythonOperator(
    task_id='get_all_informations_meteo',
    python_callable=get_all_informations_meteo
  )
    
    task_upload_on_gcp = PythonOperator(
    task_id='upload_on_gcp',
    python_callable=upload_on_gcp
  )
    
    task_download_from_gcp = PythonOperator(
    task_id='download_from_gcp',
    python_callable=download_from_gcp
  )

    task_load_to_database = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database
  )
    
task_get_all_informations_meteo >> task_upload_on_gcp >> task_download_from_gcp >> task_load_to_database