from utils.connexions import conn_psycopg
import pandas as pd
import psycopg
from io import BytesIO
from utils.connexions import bucket_meteo_files, config, psql_insert_copy, engine
from datetime import datetime
import requests
import json
import time
import re
import uuid


def get_data_from_geo_table():
  
  cities_list = [
    "Paris",
    "Lyon",
    "Marseille",
    "Bastia",
    "Orléans",
    "Calvi",
    "Brest",
    "Toulouse",
    "Lille",
    "Bordeaux"
  ]
  
  df_cities = pd.DataFrame()
  
  for citie in cities_list:
    
    with psycopg.connect(conn_psycopg) as conn:
      with conn.cursor() as cur:
        cur.execute('SET search_path TO "dev"')
        cur.execute(f"SELECT * FROM dev.geolocalisation WHERE com_name = '{citie}'")
        col_names = [desc[0] for desc in cur.description]
        result = cur.fetchall()
        df_citie = pd.DataFrame(data=result, columns=col_names)
        df_citie['city'] = citie
        df_cities = pd.concat([df_cities, df_citie], ignore_index=True)

  df_cities = df_cities[['city', 'latitude', 'longitude']]

  return df_cities, cities_list


def transform_lat_long(df_cities, cities_list):
  
  df_with_city_lat_lon_cities = df_cities.copy()
  
  df_with_city_lat_lon_cities['lat_lon'] = df_with_city_lat_lon_cities['latitude'].astype(str) + ',' + df_with_city_lat_lon_cities['longitude'].astype(str)
  
  lat_lon_object = {}
  
  for city in cities_list:
    
    city_lat_lon = df_with_city_lat_lon_cities.loc[(df_with_city_lat_lon_cities['city'] == city), 'lat_lon']
  
    if not city_lat_lon.empty:
      
      lat_lon_object[city] = city_lat_lon.iloc[0]
  
  return lat_lon_object


def get_data_from_api(lat_lon_object):
  
  url = "https://data.api.xweather.com/forecasts/"
  
  request_fields = [
    'loc.lat',
    'loc.long',
    'place.name',
    'place.country',
    'periods.timestamp',
    'periods.validTime',
    'periods.tempC',
    'periods.humidity',
    'periods.precipMM',
    'periods.pressureMB',
    'periods.windDir',   
    'periods.windSpeedKPH',    
    'periods.visibilityKM',     
]
  
  formatted_fields = []
  if request_fields is not None:
      formatted_fields = ','.join(request_fields)

  df_with_all_forecasts_data = pd.DataFrame()

  for lat_lon in lat_lon_object.values():
    
    params = {
      "p": lat_lon,
      "client_id": config['METEO_API_ID'],
      "client_secret": config['METEO_API_KEY'],
      "filter": "1hr",
      "fields": formatted_fields
    }
    
    try:
      response = requests.get(url=url, params=params)
    except Exception as e:
      raise e
    
    response_json = json.loads(response.text)
    df_pre_period = pd.json_normalize(response_json['response'][0]).drop("periods", axis=1)
    df_periods = pd.json_normalize(response_json['response'][0], "periods", record_prefix="periods.")
    df_data_meteo_forecast = df_pre_period.join(df_periods, how="cross")
    df_rename = rename_columns(df_data_meteo_forecast, request_fields)
    df_with_date = get_more_informations_from_date(df_rename)
    df_with_all_forecasts_data = pd.concat([df_with_all_forecasts_data, df_with_date], ignore_index=True)
    df_with_all_forecasts_data['id'] = df_with_all_forecasts_data.index

  return df_with_all_forecasts_data


def load_to_forecasts_table(df):
  
  start_time = time.time()
  df.to_sql(
    name="forecasts",
    schema="dev",
    con=engine,
    if_exists='replace',
    index=False,
    method=psql_insert_copy
  )
  end_time = time.time()
  total_time = end_time - start_time
  print(f"Insert time: {total_time} seconds")
  
  return df


def rename_columns(df, request_fields):
  
  columns = [col.split('.')[-1] for col in request_fields]
  columns = [re.sub(r'(?<=.)([A-Z]+)(?=[A-Z][a-z])|(?<=[a-z0-9])([A-Z])', r'_\1\2', col).lower() for col in columns]
  df.columns = columns

  return df


def get_more_informations_from_date(df):

  df['valid_time'] = pd.to_datetime(df['valid_time'])
  df['date'] = df['valid_time'].dt.date
  df['hour'] = df['valid_time'].dt.hour
  df['day'] = df['valid_time'].dt.day
  df['month'] = df['valid_time'].dt.month
  
  return df
      
      
def upload_on_gcp_bucket(df):
  
  now = datetime.now()
  datetime_without_ms = now.strftime('%Y_%m_%d_%H:%M')
  output_name = f"{datetime_without_ms}.csv"
  
  try:
    blob = bucket_meteo_files.blob(output_name)
    blob.upload_from_string(df.to_csv(sep=';', index=False))
    return output_name
  except Exception as e:
    raise Exception(f'Erreur: {e}')


def download_from_gcp_bucket(filename):
  
  blob = bucket_meteo_files.blob(filename)
  byte_stream =BytesIO()
  blob.download_to_file(byte_stream)
  byte_stream.seek(0)
  df = pd.read_csv(byte_stream, sep=";")
  
  return df

def drop_staging_schema_database():
  
  with psycopg.connect(conn_psycopg) as conn:
    with conn.cursor() as cur:
      cur.execute('DROP SCHEMA if exists staging CASCADE;')