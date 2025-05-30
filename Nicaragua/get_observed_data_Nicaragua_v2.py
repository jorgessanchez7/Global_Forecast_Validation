import os
import pandas as pd

# Ruta de la carpeta que contiene los archivos CSV
folder_path = "/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/Intermediate_Data"

# Lista para guardar resultados
streamflow_dfs = {}
waterlevel_dfs = {}

# Recorremos los archivos en la carpeta
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_code = os.path.splitext(filename)[0]  # nombre sin extensión
        file_path = os.path.join(folder_path, filename)

        # Leer CSV
        df = pd.read_csv(file_path)

        # Reemplazar valores faltantes con -9999
        df = df.fillna(-9999)

        # Convertir columna Fecha a índice datetime
        df["Datetime"] = pd.to_datetime(df["Fecha"], format="%Y-%m-%d")
        df = df.set_index("Datetime")

        # Crear DataFrame de nivel
        wl_df = df[["Nivel"]].rename(columns={"Nivel": "Water Level (m)"})
        # Crear DataFrame de caudal
        q_df = df[["Caudal"]].rename(columns={"Caudal": "Streamflow (m3/s)"})

        wl_df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/Data/{}_WL.csv".format(file_code))
        q_df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/Data/{}_Q.csv".format(file_code))

        # Guardar usando el código del archivo
        streamflow_dfs[file_code] = q_df
        waterlevel_dfs[file_code] = wl_df

        print(streamflow_dfs)
        print(waterlevel_dfs)