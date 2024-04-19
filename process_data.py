import time
import os
import polars as pl
from device_group import generate_df_device_group_by_month
from xdr_simplified import generate_df_xdr_simplified
from device_trajectory import generate_trajectories
from od_by_group import generate_od_by_group
from process_od_df import process_od_df

raw_data_path = "../data_tesis/data_geo"
output_path = "../data_tesis/output"
quantile = "cuartil"

# año 2015
# week : [l, m, m, j, v]
# weekend: [s, d]
month_to_days = {
    "03": {
        "week": ["16", "17", "18", "19" , "20"],
        "weekend": ["21", "15"]
    },
    "05": { 
        "week": ["11", "12", "13", "14", "15"],
        "weekend": ["16", "10"]
    },
    "08": {
        "week": ["03", "04", "05", "06", "07"],
        "weekend": ["08", "02"]
    },
    "11": {
        "week": ["23", "24", "25", "26", "20"],
        "weekend":["21", "22"]
    }
}
start_time = time.perf_counter()

for month in month_to_days:
    device_group_file_path = f"{output_path}/device_ismt_group_{month}.csv"
    if not os.path.isfile(device_group_file_path):
        print("Calculando cuantiles ISMT para cada dispositivo... ")
        generate_df_device_group_by_month(
            residence_file_path=f"{raw_data_path}/residence_estimation_mfl_{month}.csv",
            ismt_file_path=f"{raw_data_path}/data_ch_ismt.csv",
            output_file_path=device_group_file_path,
            )
        
    days = month_to_days[month]["week"]

    for day in days:
        date = month + day # MMDD
        xdr_simplified_file_path = f"{output_path}/xdr_simplified_{date}.csv"
        device_trajectory_file_path = f"{output_path}/device_trajectories_{date}.csv"

        if not os.path.isfile(xdr_simplified_file_path):
            print(f"Obteniendo datos XDR simplificados para la fecha {date}...")
            generate_df_xdr_simplified(
                xdr_raw_file_path=f"{raw_data_path}/new_geo_data_{date}.csv",
                device_id_file_path=f"{raw_data_path}/users_oldid_2_newid.csv",
                output_file_path=xdr_simplified_file_path,
                month=month
                )

        
        if not os.path.isfile(device_trajectory_file_path):
            print(f"Generando trayectorias para la fecha {date}... ")
            generate_trajectories(
                xdr_simplified_file_path=xdr_simplified_file_path,
                output_file_path=device_trajectory_file_path
            )

# se opera en este orden para no tener que usar tanta memoria secundaria al mismo tiempo
for start_hour in range(24):
    for end_hour in range(start_hour, 24):

        base_path = f"{output_path}/od_{start_hour}_{end_hour}_{quantile}"

        # ignorar si ya se calculó esta combinación antes
        if os.path.isfile(f"{base_path}.csv"):
            continue

        print(f"Generando flujos OD entre horas {start_hour} y  {end_hour}")
        for month in month_to_days:
            
            # primero, generar los flujos OD para cada día
            days = month_to_days[month]["week"]
            days_od_path_list = [f"{base_path}{month + day}.csv" for day in days]

            for od_by_group_file_path in days_od_path_list:

                generate_od_by_group(
                    device_trajectory_file_path=device_trajectory_file_path,
                    device_group_file_path=device_group_file_path,
                    output_file_path= od_by_group_file_path,
                    start_hour=start_hour,
                    end_hour=end_hour,
                    quantile=quantile
                )
            
            # agergación de flujos de cada día de este mes según count
            df_od_week_concat = pl.concat(list(map(pl.read_csv, days_od_path_list)))
            df_od_week = df_od_week_concat.group_by(
                ["lat_O", "lon_O", "lat_D", "lon_D", "group"]
            ).agg(pl.sum("count"))

            try:
                df_od_week.write_csv(f"{base_path}_{month}.csv")
            except Exception as e:
                print(e)
            
            # limpiar archivos temporales de los días
            for path in days_od_path_list:
                os.remove(path)
        
        # ahora agregación de flujos de cada mes según count
        months_od_path_list = [f"{base_path}_{month}.csv" for month in month_to_days]  
        df_od_month_concat = pl.concat(list(map(pl.read_csv, months_od_path_list)))
        df_od = df_od_month_concat.group_by(
                ["lat_O", "lon_O", "lat_D", "lon_D", "group"]
            ).agg(pl.sum("count"))
        
        #
        process_od_df(
            df_od=df_od,
            output_file_path=f"{base_path}.csv"
        )
        
        # limpiar archivos temporales de los meses
        for path in months_od_path_list:
            os.remove(path)

end_time = time.perf_counter()

print(f"Tiempo transcurrido: {(end_time - start_time):.2f} segundos")
