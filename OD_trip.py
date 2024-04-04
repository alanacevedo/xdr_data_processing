import polars as pl

FILE_PATH = "../data_tesis/data_geo"
OUTPUT_PATH = "../data_tesis/output"
date = "0315" # MMDD

# para observar un dataframe, usar print(df.fetch(streaming=True).head())

# los archivos new_geo_data_MMDD no tienen headers
col_names = ['fecha', 'hora', 'duracion', 'device_id_str', 'rinhead', 'rintail', 'custom_tower', 'lat', 'lon', 'minvu']
df_raw_data = pl.scan_csv(f"{FILE_PATH}/new_geo_data_{date}.csv",
                          has_header=False,
                          new_columns=col_names
                          )

# parsear las columnas fecha y hora para crear un datetime
date_string = pl.col("fecha").cast(pl.Utf8)
hour_string = pl.col("hora").cast(pl.Utf8).str.zfill(6)
df_parsed_date= df_raw_data.with_columns([(date_string + hour_string).str.to_datetime("%y%m%d%H%M%S").alias("datetime")])

# mapeo de id viejo (str) a id nuevo (int)
df_ids = pl.scan_csv(f"{FILE_PATH}/users_oldid_2_newid.csv", separator=";", new_columns=["device_id", "old_device_id_str"])

# join de ambos df en el string identificador del dispositivo
df_device_id_joined = df_parsed_date.join(df_ids, left_on="device_id_str", right_on="old_device_id_str")

# Seleccionar sólo las columnas id_int, datetime, y latlon
df_xdr = df_device_id_joined.select(pl.col("device_id"), pl.col("datetime"), pl.col("lat"), pl.col("lon"))

# Filtrar solo las posiciones entre 5 y 13 hrs, para trabajar solo con trayectorias de "ida" al lugar de trabajo
df_morning_xdr = df_xdr.filter(pl.col("datetime").dt.hour().is_between(5, 12))

# print(df_morning_xdr.fetch(streaming=True).head())
# df_morning_xdr ya se encuentra ordenado tanto por device_id como por datetime.
# Se toman las coordenadas del primer y último lugar visitado. 
df_OD = df_morning_xdr.group_by("device_id").agg(
    [pl.col("lat").first().alias("lat_start"),
     pl.col("lon").first().alias("lon_start"),
     pl.col("lat").last().alias("lat_end"),
     pl.col("lon").last().alias("lon_end")])

# ignorar trayectorias que terminan en el mismo lugar
df_filtered_OD = df_OD.sort("device_id").filter((pl.col("lat_start") != pl.col("lat_end")) | (pl.col("lon_start") != pl.col("lon_end")))


#"""
print("start")
try:
    df_filtered_OD.sink_csv(f"{OUTPUT_PATH}/OD_trips_{date}.csv")
except Exception as e:
    print(e)

print("finish")
#"""
