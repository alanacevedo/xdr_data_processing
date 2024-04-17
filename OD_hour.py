import polars as pl


"""
A partir de las trayectorias de los dispositivos y de los grupos de cada dispositivo, genera los flujos OD de cada grupo
entre las horas start_hour y end_hour. Agrupa según el cuartil {quantile}, que debe existir en el archivo device_group_file.
"""
def generate_OD_by_group_and_hour(device_trajectory_file_path, device_group_file_path, output_file_path, start_hour, end_hour, quantile):
    
    # ismt y cuantil de cada device
    df_device_group = pl.read_csv(device_group_file_path)
    df_device_trajectory = pl.read_csv(device_trajectory_file_path)

    # obtener el cuantil / grupo correspondiente
    df_device_trajectory = df_device_trajectory.join(df_device_group, left_on="device_id", right_on="device_id")

    # seleccionar el grupo, y latlon de origen y desitno.
    # es end_hour + 1 ya que al final de una hora x coincide con el inicio de una hora x+1
    df_OD_by_group = df_device_trajectory.select(
        pl.col(quantile).alias("group"),
        pl.col(f"lat_{start_hour}").alias("lat_O"),
        pl.col(f"lon_{start_hour}").alias("lon_O"),
        pl.col(f"lat_{end_hour+1}").alias("lat_D"),
        pl.col(f"lon_{end_hour+1}").alias("lon_D")
        )
    
    # agregación por grupo y por latlon de origen y destino
    df_OD_count_by_group = df_OD_by_group.group_by(
        ["lat_O", "lon_O", "lat_D", "lon_D", "group"]
    ).agg(pl.col("group").count().alias("count"))
    
    try:
        df_OD_count_by_group.write_csv(output_file_path)
    except Exception as e:
        print(e)

    return



#testing
date = "0316"
month = "03"
start_hour = 5
end_hour = 12
quantile="cuartil"

generate_OD_by_group_and_hour(
    device_trajectory_file_path=f"../data_tesis/output/device_trajectory_{date}.csv",
    device_group_file_path=f"../data_tesis/output/device_ismt_group_{month}.csv",
    output_file_path=f"../data_tesis/output/OD_hour_{start_hour}_{end_hour}_group_{quantile}.csv",
    start_hour=start_hour,
    end_hour=end_hour,
    quantile=quantile
    )
