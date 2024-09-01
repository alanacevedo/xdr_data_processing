from geopy import distance
import polars as pl
import h3.api.basic_str as h3
import numpy as np
import os


def get_distance_vectorized(h3_O, h3_D):
    latlon_O = np.array([h3.h3_to_geo(h) for h in h3_O])
    latlon_D = np.array([h3.h3_to_geo(h) for h in h3_D])
    return np.array([distance.distance(o, d).km for o, d in zip(latlon_O, latlon_D)])

def process_od_df(df_od: pl.DataFrame, output_path: str, start_hour: int, end_hour: int, quantile: str):
    if not len(df_od):
        return
    
    print(f"Processing OD for hours {start_hour}, {end_hour}")

    for res in [5, 6, 7, 8, 9]:
        directory = f"{output_path}/h3_{res}"
        path = f"{directory}/od_{start_hour}_{end_hour}_{quantile}.csv"
        if os.path.exists(path):
            
            continue

        print(f"Generating h3 OD with resolution {res}")

        # Vectorized H3 index calculation
        df_od = df_od.with_columns([
            pl.struct(["lat_O", "lon_O"]).map_elements(lambda x: h3.geo_to_h3(x['lat_O'], x['lon_O'], res)).alias('h3_O'),
            pl.struct(["lat_D", "lon_D"]).map_elements(lambda x: h3.geo_to_h3(x['lat_D'], x['lon_D'], res)).alias('h3_D')
        ])

        # Group by and aggregate
        od_by_h3 = df_od.group_by(["h3_O", "h3_D", "group"]).agg(pl.col("count").sum())

        # Calculate distances
        distances = get_distance_vectorized(od_by_h3['h3_O'], od_by_h3['h3_D'])
        od_by_h3 = od_by_h3.with_columns(pl.Series('distance', distances))

        # Sort by distance
        od_by_h3 = od_by_h3.sort('distance')

        # Calculate total count
        total_count = od_by_h3["count"].sum()

        # Normalize by total
        od_by_h3 = od_by_h3.with_columns((pl.col("count") / total_count).alias("norm_total"))

        # Calculate group counts
        group_counts = od_by_h3.group_by("group").agg(pl.col("count").sum()).sort("group")

        # Normalize by group
        od_by_h3 = od_by_h3.join(group_counts, on="group", how="left")
        od_by_h3 = od_by_h3.with_columns((pl.col("count") / pl.col("count_right")).alias("norm_group"))
        od_by_h3 = od_by_h3.drop("count_right")

        if not os.path.exists(directory):
            os.makedirs(directory)

        try:
            od_by_h3.write_csv(path)
        except Exception as e:
            print(e)

    return
        

if __name__ == "__main__":
    output_path = "../data_tesis/output"
    test_path = f"{output_path}/od_6_14_cuartil_unprocessed.csv"
    unprocessed_df = pl.read_csv(test_path)
    process_od_df(unprocessed_df, output_path, 6, 14, "cuartil")