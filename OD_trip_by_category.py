import polars as pl


""""
"""
def generate_df_OD_trip_by_category(OD_trips_path_list, device_group_path, quantile, output_file_path):

    # combinar los días en 1 solo dataframe
    df_OD_merged = pl.concat(list(map(pl.read_csv, OD_trips_path_list))).sort("device_id")

    # ismt y cuantil de cada device
    df_device_group = pl.read_csv(device_group_path)

    df_joined = df_OD_merged.join(df_device_group, left_on="device_id", right_on="device_id")

    # agregación según cuantil y OD
    df_aggregated = df_joined.group_by(
        ["lat_start", "lon_start", "lat_end", "lon_end", quantile] # cambiar por cuantil deseado
    ).agg(pl.col("device_id").count().alias("count")).sort(by=pl.col("count"), descending=True)

    try:
        df_aggregated.write_csv(output_file_path)
    except Exception as e:
        print(e)

    return