import csv

"""
Genera un csv donde cada fila contiene la ubicación de un dispositivo a cada hora del día.

Tener en consideración: estoy asumiendo que se tienen TODOS los pings de cada dispositivo.
Por esto puedo asumir que en horas que no tengan datos, está en el mismo lugar que antes
"""
def generate_trajectories(xdr_simplified_file_path, output_file_path):

    with open(xdr_simplified_file_path, "r", newline='') as xdr_hour_file, open(output_file_path, "w", newline='') as output_file:

        # device_id, hour, lat, lon
        reader = csv.reader(xdr_hour_file) 
        writer = csv.writer(output_file)

        headers = ["device_id"]
        # se incluye la hora 24 para poder calcular trayectorias que terminen al final de la hora 23
        for hour in range(25):
            headers.extend([f"lat_{hour}", f"lon_{hour}"])

        writer.writerow(headers)

        curr_device = None
        curr_trajectory = []
        curr_hour = 0
        last_pos = None, None
        
        # ignore headers
        next(reader)

        for row in reader:
            device_id, hour, lat, lon = row
            hour = int(hour)

            # nuevo dispositivo, escribir trayectoria del dispositivo anterior y resetear variables
            if device_id != curr_device:
                if curr_device is not None:
                    while curr_hour < 25:
                        curr_trajectory.extend(last_pos)
                        curr_hour += 1

                    writer.writerow(curr_trajectory)

                curr_device = device_id
                curr_trajectory = [device_id]
                curr_hour = 0
                last_pos = None, None

            
            if hour != curr_hour:
                # ha cambiado de hora. Entre la última hora y la actual (excluyente), el dispositivo se ha encontrado en last_pos

                if last_pos == (None, None):
                    # voy a asumir que a las 00 hrs el dispositivo se encontraba donde aparece el primer dato xdr de ese día
                    # quizás debería dejar Null para el primer día de la semana, y para los demás usar el último valor del día anterior
                    # el problema es que en los distintos días no siempre están los mismos dispositivos
                    last_pos = lat, lon
                
                while curr_hour < hour:
                    curr_trajectory.extend(last_pos)
                    curr_hour += 1
            
            last_pos = lat, lon
            

"""
#testing
date = "0316"
generate_trajectory(
    xdr_hour_file_path=f"../data_tesis/output/xdr_hour_{date}.csv",
    output_file_path=f"../data_tesis/output/device_trajectory_{date}.csv"
)
"""