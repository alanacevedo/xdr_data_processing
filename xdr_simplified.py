import polars as pl

# para observar un dataframe, usar print(df.fetch(streaming=True).head(n=20))

"""
Mapea los datos brutos xdr a datos xdr que constan solo de id_device (int), latlon, y hora. 

En los archivos new_geo_data_{date}.csv, las entradas vienen juntas según device_id, y con los datetime ya ordenados cronológicamente.
Esta función no altera el orden en el que vienen.

Las operaciones generales, incluyendo filter y group_by de polars mantienen el orden relativo.
https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.group_by.html#polars.DataFrame.group_by
https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.filter.html#polars.DataFrame.filter
Esto permite evitar tener que hacer un sort.
"""
def generate_df_xdr_simplified(xdr_raw_file_path, device_id_file_path, output_file_path):


    # los archivos new_geo_data_MMDD no tienen headers
    col_names = ['fecha', 'hora', 'duracion', 'device_id_str', 'rinhead', 'rintail', 'custom_tower', 'lat', 'lon', 'minvu']
    df_raw_data = pl.scan_csv(xdr_raw_file_path,
                            has_header=False,
                            new_columns=col_names
                            )

    # parsear las columnas fecha y hora para crear un datetime
    date_string = pl.col("fecha").cast(pl.Utf8)
    hour_string = pl.col("hora").cast(pl.Utf8).str.zfill(6)
    df_parsed_date= df_raw_data.with_columns([(date_string + hour_string).str.to_datetime("%y%m%d%H%M%S").alias("datetime")])

    # mapeo de id viejo (str) a id nuevo (int)
    df_ids = pl.scan_csv(device_id_file_path, separator=";", new_columns=["device_id", "old_device_id_str"])

    # join de ambos df en el string identificador del dispositivo
    df_device_id_joined = df_parsed_date.join(df_ids, left_on="device_id_str", right_on="old_device_id_str")


    # obtener hora a partir del datetime. Tener en cuenta que se mantiene orden cronológico
    df_device_id_joined_with_hour = df_device_id_joined.with_columns(pl.col("datetime").dt.hour().alias("hour"))

    # Seleccionar solo datos de interés
    df_xdr = df_device_id_joined_with_hour.select(pl.col("device_id"), pl.col("hour"), pl.col("lat"), pl.col("lon"))
    
    try:
        df_xdr.sink_csv(output_file_path)
    except Exception as e:
        print(e)
    
    return

"""
#testing
date = "0316"
generate_df_xdr_hour(
    xdr_raw_file_path=f"../data_tesis/data_geo/new_geo_data_{date}.csv",
    device_id_file_path=f"../data_tesis/data_geo/users_oldid_2_newid.csv",
    output_file_path=f"../data_tesis/output/xdr_hour_{date}.csv"
)
"""