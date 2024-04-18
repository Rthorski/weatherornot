from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import datetime
from weather_or_not.weather_or_not_controller import get_all_informations_meteo, upload_on_gcp, download_from_gcp, load_to_database, drop_staging_schema

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

    task_drop_staging_schema = PythonOperator(
    task_id='drop_staging_schema',
    python_callable=drop_staging_schema
  )

    task_load_to_database = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database
  )
    
    task_trigger = TriggerDagRunOperator(
      task_id='trigger_dbt_jobs_dag',
      trigger_dag_id='dbt_jobs'
    )
    
task_get_all_informations_meteo >> task_upload_on_gcp >> task_download_from_gcp >>task_drop_staging_schema >> task_load_to_database >> task_trigger