import polars as pl

FILE_PATH = "../data_tesis/data_geo"
OUTPUT_PATH = "../data_tesis/output"

month = "03"
days = ["16", "17", "18", "19" , "20"] # lunes a viernes, año 2015
cuantil = "cuartil"

def read_csv(day):
    return pl.read_csv(f"{OUTPUT_PATH}/OD_trips_{month}{day}.csv")

# combinar los 5 días en 1 solo dataframe
df_OD_merged = pl.concat(list(map(read_csv, days))).sort("device_id")

# ismt y cuantil de cada device
df_device_group = pl.read_csv(f"{OUTPUT_PATH}/device_ismt_group_{month}.csv")

df_joined = df_OD_merged.join(df_device_group, left_on="device_id", right_on="device_id")

# agregación según cuantil y OD
df_aggregated = df_joined.group_by(
    ["lat_start", "lon_start", "lat_end", "lon_end", cuantil] # cambiar por cuantil deseado
).agg(pl.col("device_id").count().alias("count")).sort(by=pl.col("count"), descending=True)

#"""
print("start")
try:
    df_aggregated.write_csv(f"{OUTPUT_PATH}/OD_by_{cuantil}_{month}.csv")
except Exception as e:
    print(e)

print("finish")
#"""
