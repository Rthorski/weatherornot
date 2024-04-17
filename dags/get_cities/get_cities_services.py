from utils.connexions import bucket_name, storage_client, conn_psycopg
from io import BytesIO
import pandas as pd
import psycopg

def download_file():
  
  try:
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
      blob_bytes = blob.download_as_bytes()
      blob_io = BytesIO(blob_bytes)
      return blob_io
  except Exception as e:
    print(f"Erreur lors du téléchargement du fichier dans le storage: {e}")


def readCsv(blob_io):
  
  df = pd.read_csv(blob_io, sep=";")
  
  return df


def dropColumns(df):
  
  df = df[["dep_name", 'reg_name','com_code', 'com_name', "com_name_upper", "com_name_lower", 'geo_point_2d']]
  
  return df


def splitGeoColumns(df):

  df[['latitude', 'longitude']] =  df['geo_point_2d'].str.split(expand=True)
  df['latitude'] = df['latitude'].str.replace(',', '')
  df['latitude'] = pd.to_numeric(df['latitude'], downcast='float')
  df['longitude'] = pd.to_numeric(df['longitude'], downcast='float')
  df.drop(columns=['geo_point_2d'], inplace=True)
  
  return df


def transformDfInObjectJson(df):
  
  df_json = df.to_dict(orient='records')
  
  return df_json


def getValuesFromDict(object_json):
  
  dict_values = [tuple(d.values()) for d in object_json]

  return dict_values


def loadGeoInDataBase(dict_values):
  
  columns = "dep_name, reg_name, com_code, com_name, com_name_upper, com_name_lower, latitude, longitude"
  
  try:
    
    with psycopg.connect(conn_psycopg) as conn:
      with conn.cursor() as cur:
        cur.execute('SET search_path TO "dev"')
        cur.execute("SELECT count(*) FROM dev.geolocalisation")
        print("La table existe")
        
  except:
    
    try: 
      with psycopg.connect(conn_psycopg) as conn:
        with conn.cursor() as cur:
          cur.execute("SET search_path TO 'dev'")
          query = """
          CREATE TABLE geolocalisation (
            dep_name VARCHAR(255),
            reg_name VARCHAR(255),
            com_code VARCHAR(255),
            com_name VARCHAR(255),
            com_name_upper VARCHAR(255),
            com_name_lower VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
          )
          """
          cur.execute(query)
          
          with cur.copy(f"COPY geolocalisation ({columns}) FROM STDIN") as copy:
            for record in dict_values:
              copy.write_row(record)
              
    except (psycopg.DatabaseError, Exception) as e:
      raise Exception(f"Erreur= {e}")