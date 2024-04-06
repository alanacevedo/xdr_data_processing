import polars as pl

FILE_PATH = "../data_tesis/data_geo"
OUTPUT_PATH = "../data_tesis/output"
month = "03"

df_raw_residence = pl.read_csv(f"{FILE_PATH}/residence_estimation_mfl_{month}.csv", separator=";")
df_residence = df_raw_residence.select(
    pl.col("ID").alias("device_id"),
    pl.col("home").alias("home_antenna_id")
).filter(pl.col("home_antenna_id") != -1)

df_raw_ismt = pl.read_csv(f"{FILE_PATH}/data_ch_ismt.csv") # descargado de clickhouse
df_ismt = df_raw_ismt.select(
    pl.col("id").alias("antenna_id"),
    pl.col("valor").alias("ismt")
)

df_device_ismt = df_residence.join(df_ismt, left_on="home_antenna_id", right_on="antenna_id").select(pl.exclude("home_antenna_id"))

# qcut calcula los cuantiles
df_device_group = df_device_ismt.with_columns(
    pl.col("ismt").qcut(4, labels=["1", "2", "3", "4"]).alias("cuartil"),
    pl.col("ismt").qcut(5, labels=["1", "2", "3", "4", "5"]).alias("quintil"))


#print(df_device_group.fetch(streaming=True).head())

# usé read y write csv en vez de scan y sink porque me tiraba error, además los archivos no son tan
# grandes como para necesitar scan de memoria secundaria.
#"""
print("start")
try:
    df_device_group.write_csv(f"{OUTPUT_PATH}/device_ismt_group_{month}.csv")
except Exception as e:
    print(e)

print("finish")
#"""