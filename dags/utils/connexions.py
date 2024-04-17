from dotenv import dotenv_values
from sqlalchemy import create_engine
from google.cloud import storage
import csv
from io import StringIO


config = dotenv_values('/opt/airflow/dags/.env')
engine = create_engine(f'postgresql+psycopg2://{config["POSTGRES_USER"]}:{config["POSTGRES_PASSWORD"]}@{config["POSTGRES_HOST"]}:5432/{config["POSTGRES_DB"]}')

conn_psycopg = f'postgresql://{config["POSTGRES_USER"]}:{config["POSTGRES_PASSWORD"]}@{config["POSTGRES_HOST"]}:5432/{config["POSTGRES_DB"]}'

def authentificateServiceAccount():
  
  credentials = config["GOOGLE_APPLICATION_CREDENTIALS"]
  
  try:
    storage_client = storage.Client.from_service_account_json(credentials)
  except Exception as e:
    print(f"Erreur: {e}")
    
  return storage_client

storage_client = authentificateServiceAccount()
bucket_name = 'geolocalisation_cities'
bucket_geolocalisation_cities = storage_client.bucket(bucket_name)
bucket_meteo_files = storage_client.bucket('meteo_files')


def psql_insert_copy(table, conn, keys, data_iter): #mehod
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)