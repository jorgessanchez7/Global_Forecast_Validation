import os
import xarray as xr
import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings("ignore")

# === CONFIGURACIÓN ===
fecha_ini = dt.datetime(2024, 10, 10)
fecha_fin = dt.datetime(2024, 11, 10)
fechas = pd.date_range(fecha_ini, fecha_fin, freq='12H')
fechas = fechas.strftime("%Y-%m-%d %H")

base_folder = r"G:\My Drive\Personal_Files\Post_Doc\GEOGLOWS_Applications\Spain\Forecast_Values_EFAS"
stations_pd = pd.read_csv(r"G:\My Drive\Personal_Files\Post_Doc\GEOGLOWS_Applications\Spain\Coordenadas_POI.csv")

latitudes = stations_pd['Lat_EFAS'].to_list()
longitudes = stations_pd['Lon_EFAS'].to_list()

# === BUCLE PRINCIPAL ===
for fecha in fechas:

    # rutas de entrada para los tres archivos netCDF
    files = [
        fr"D:\EFAS_Forecast_2\{fecha}\data_0.nc",
        fr"D:\EFAS_Forecast_2\{fecha}\data_1.nc",
        fr"D:\EFAS_Forecast_2\{fecha}\data_2.nc"
    ]

    for latitude, longitude in zip(latitudes, longitudes):

        print(f"{latitude} - {longitude} - {fecha}")

        # ruta de salida
        file_path = os.path.join(base_folder, f"{fecha}", f"{latitude:.6f}_{longitude:.6f}.csv".replace("+", ""))
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        forecast_df = pd.DataFrame()
        ensemble_counter = 1

        # === Leer los tres archivos ===
        for file in files:
            if not os.path.exists(file):
                print(f"Archivo no encontrado: {file}")
                continue

            try:
                with xr.open_dataset(file) as ds:
                    forecast_datetimes = pd.to_datetime(ds['valid_time'].values)

                    # seleccionar la descarga más cercana a la coordenada
                    discharge = ds['dis06'].sel(latitude=latitude, longitude=longitude, method='nearest')

                    # construir un DataFrame temporal con índice temporal
                    temp_df = pd.DataFrame(index=forecast_datetimes)

                    if 'number' in discharge.dims:
                        # múltiples ensambles
                        for ens in discharge['number'].values:
                            col_name = f"ensemble_{ensemble_counter:02d}"
                            temp_df[col_name] = discharge.sel(number=ens).values
                            ensemble_counter += 1
                    else:
                        # control forecast
                        col_name = f"ensemble_{ensemble_counter:02d}"
                        temp_df[col_name] = discharge.values
                        ensemble_counter += 1

                # === Unificar horizontes temporales (mantener el más largo) ===
                if forecast_df.empty:
                    forecast_df = temp_df
                else:
                    # usar la unión de índices (mantiene NaN donde falte información)
                    union_index = forecast_df.index.union(temp_df.index)
                    forecast_df = forecast_df.reindex(union_index)
                    temp_df = temp_df.reindex(union_index)
                    forecast_df = pd.concat([forecast_df, temp_df], axis=1)

            except Exception as e:
                print(f"Error leyendo {file}: {e}")
                continue

        # reemplazar negativos por 0 (sin afectar NaN)
        forecast_df[forecast_df < 0] = 0

        # ordenar por fecha y guardar
        forecast_df = forecast_df.sort_index()

        if not forecast_df.empty:
            forecast_df.to_csv(file_path)
        else:
            print(f"No se generó pronóstico válido para {latitude}, {longitude} en {fecha}")
