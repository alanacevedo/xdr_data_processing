import polars as pl

"""
A partir de la antena hogar estimada para cada usuario, y el ismt estimado para cada antena, se 
calcula el cuantil ISMT de cada usuario.
"""
def generate_df_device_group_by_month(residence_file_path, ismt_file_path, output_file_path):
    df_raw_residence = pl.read_csv(residence_file_path, separator=";")
    df_residence = df_raw_residence.select(
        pl.col("ID").alias("device_id"),
        pl.col("home").alias("home_antenna_id")
    ).filter(pl.col("home_antenna_id") != -1)

    df_raw_ismt = pl.read_csv(ismt_file_path) # descargado de clickhouse
    df_ismt = df_raw_ismt.select(
        pl.col("id").alias("antenna_id"),
        pl.col("valor").alias("ismt")
    )

    df_device_ismt = df_residence.join(df_ismt, left_on="home_antenna_id", right_on="antenna_id").select(pl.exclude("home_antenna_id"))

    # qcut calcula los cuantiles
    df_device_group = df_device_ismt.with_columns(
        pl.col("ismt").qcut(4, labels=["1", "2", "3", "4"]).alias("cuartil"),
        pl.col("ismt").qcut(5, labels=["1", "2", "3", "4", "5"]).alias("quintil"))
    

    # usé read y write csv en vez de scan y sink porque me tiraba error, además los archivos no son tan
    # grandes como para necesitar scan de memoria secundaria.   
    try:
        df_device_group.write_csv(output_file_path)
    except Exception as e:
        print(e)

    return


