from geopy import distance
import polars as pl
from functools import cache

@cache
def get_od_distance(latlon_o, latlon_d):
    return distance.distance(latlon_o, latlon_d).km

def get_row_od_distance(tuple):
    lat_o, lon_o, lat_d, lon_d, _, _ = tuple
    return get_od_distance((lat_o, lon_o), (lat_d, lon_d))

def get_row_group_normalization(group_count_series, tuple):
    _, _, _, _, group, count, _, _ = tuple
    # norm_group_series es básicamente un arreglo [count(cuantil 1), count(cuantil 2), ...] que ya viene ordenado
    # group va desde 1 hacia (total_cuantiles)
    return count / group_count_series[group-1]

"""
Filtra los OD según count y luego agrega la distancia entre el origen y el destino.
Agrega el valor de count normalizado.
Finalmente ordena según distancia.
"""
def process_od_df(df_od: pl.DataFrame, output_file_path):
    if not len(df_od): return
    
    # no considerar aquellos OD cuyo count es bajo, para reducir cantidad de nodos
    # arbitrariamente decidí que no se consideren los que tengan count dentro del 20% más bajo
    
    distinct_counts = df_od["count"].unique()
    threshold_index = int(0.2 * len(distinct_counts))
    threshold = distinct_counts.sort()[threshold_index]

    df_od_filtered = df_od.filter(pl.col("count") > threshold)
    distance_col = df_od_filtered.map_rows(get_row_od_distance)
    df_od_distance = pl.concat([df_od_filtered, distance_col], how="horizontal").rename({"map": "distance"})
    df_od_sorted = df_od_distance.sort(pl.col("distance"))

    # valor normalizado total, para evitar calcularlo en la plataforma web
    total_count = df_od_sorted["count"].sum()
    df_od_norm_total = df_od_sorted.with_columns((pl.col("count") / total_count).alias("norm_total"))

    # valor normalizado por grupo
    group_count_series = df_od_sorted.group_by("group").agg(pl.col("count").sum()).sort(pl.col("group"))["count"]
    norm_group_col = df_od_norm_total.map_rows(lambda t: get_row_group_normalization(group_count_series, t))
    df_od_norm_group = pl.concat([df_od_norm_total, norm_group_col], how="horizontal").rename({"map": "norm_group"})

    try:
        df_od_norm_group.write_csv(output_file_path)
    except Exception as e:
        print(e)
    
    return
