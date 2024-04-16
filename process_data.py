import time
from device_group import generate_df_device_group_by_month
from OD_trip import generate_df_OD_trip
from OD_trip_by_category import generate_df_OD_trip_by_category

raw_data_path = "../data_tesis/data_geo"
output_path = "../data_tesis/output"
month = "03"
days = ["16", "17", "18", "19" , "20"] # lunes a viernes, año 2015
quantile = "cuartil"

start_time = time.perf_counter()

print("Calculando cuantiles ISMT para cada dispositivo... ", end="")
generate_df_device_group_by_month(
    residence_file_path=f"{raw_data_path}/residence_estimation_mfl_{month}.csv",
    ismt_file_path=f"{raw_data_path}/data_ch_ismt.csv",
    output_file_path=f"{output_path}/device_ismt_group_{month}.csv",
    )
print("Hecho")

for day in days:
    date = month + day # MMDD
    print(f"Calculando trayectorias OD para la fecha {date}...", end="")
    generate_df_OD_trip(
        xdr_raw_file_path=f"{raw_data_path}/new_geo_data_{date}.csv",
        device_id_file_path=f"{raw_data_path}/users_oldid_2_newid.csv",
        output_file_path=f"{output_path}/OD_trips_{date}.csv"
        )

    end_time = time.perf_counter()
    print("Hecho")

print(f"Calculando trayectorias OD con cuantil {quantile} para el mes {month}", end="")
generate_df_OD_trip_by_category(
    OD_trips_path_list=[f"{output_path}/OD_trips_{month}{day}.csv" for day in days],
    device_group_path=f"{output_path}/device_ismt_group_{month}.csv",
    quantile=quantile,
    output_file_path=f"{output_path}/OD_by_{quantile}_{month}.csv"
)
print("Hecho")

print(f"Tiempo transcurrido: {(end_time - start_time):.2f} segundos")
