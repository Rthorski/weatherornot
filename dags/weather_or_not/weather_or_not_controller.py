from weather_or_not.weather_ot_not_services import get_data_from_geo_table, transform_lat_long, get_data_from_api, upload_on_gcp_bucket, download_from_gcp_bucket, load_to_forecasts_table, drop_staging_schema_database

def get_all_informations_meteo(**kwargs):
  
  ti = kwargs['ti']
  cities_data, cities_list = get_data_from_geo_table()
  lat_lon_object = transform_lat_long(cities_data, cities_list)
  df_with_all_forecasts_data = get_data_from_api(lat_lon_object)
  ti.xcom_push(key='df_with_all_forecasts_data', value=df_with_all_forecasts_data)


def upload_on_gcp(**kwargs):
  
  ti = kwargs['ti']
  df_with_all_forecasts_data = ti.xcom_pull(key='df_with_all_forecasts_data', task_ids="get_all_informations_meteo")
  filename = upload_on_gcp_bucket(df_with_all_forecasts_data)
  ti.xcom_push(key='filename', value=filename) 


def download_from_gcp(**kwargs):
  
  ti = kwargs['ti']
  filename = ti.xcom_pull(key='filename', task_ids="upload_on_gcp")
  df_from_download = download_from_gcp_bucket(filename)
  ti.xcom_push(key='df_from_download', value=df_from_download)

def drop_staging_schema():
  drop_staging_schema_database()

def load_to_database(**kwargs):
  
  ti = kwargs['ti']
  df = ti.xcom_pull(key='df_from_download', task_ids="download_from_gcp")
  load_to_forecasts_table(df)
  
