from get_cities.get_cities_services import download_file, readCsv, dropColumns, splitGeoColumns, transformDfInObjectJson, getValuesFromDict, loadGeoInDataBase

def get_cities_controller():
  
  blob_io = download_file()
  df = readCsv(blob_io)
  df = dropColumns(df)
  df = splitGeoColumns(df)
  df_json = transformDfInObjectJson(df)
  df_values = getValuesFromDict(df_json)
  loadGeoInDataBase(df_values)