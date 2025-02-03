import pandas as pd
import datetime as dt

stations_country = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/El_Salvador_new_Stations.csv")

IDs = stations_country.index.to_list()
Codes = stations_country['samplingFeatureCode'].to_list()
Names = stations_country['name'].to_list()
Latitudes = stations_country['latitude'].to_list()
Longitudes = stations_country['longitude'].to_list()
Elevations = stations_country['elevation_m'].to_list()
Institutions = stations_country['county'].to_list()

for id, code, name, latitude, longitude, elevation, institution in zip(IDs, Codes, Names, Latitudes, Longitudes, Elevations, Institutions):

    print(code, ' - ', name, ' - ', latitude, ' - ', longitude, ' - ', elevation, 'm')

    # Ruta del archivo Excel
    ruta_caudal = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/El_Salvador/Datos_Temporales/raw_data/Caudales Medios Diarios/{0}.csv".format(code)
    ruta_nivel = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/El_Salvador/Datos_Temporales/raw_data/Niveles Medios Diarios/{0}.csv".format(code)

    # Leer el archivo de caudales
    df_Q = pd.read_csv(ruta_caudal, skiprows=14)
    df_Q.columns = ["ISO_UTC", "Local_Timestamp", "Streamflow (m3/s)"]
    df_Q["ISO_UTC"] = pd.to_datetime(df_Q["ISO_UTC"])
    df_Q["Datetime"] = pd.to_datetime(df_Q["ISO_UTC"]).dt.strftime("%Y-%m-%d")
    df_Q["Datetime"] = pd.to_datetime(df_Q["Datetime"])
    df_Q = df_Q[["Datetime", "Streamflow (m3/s)"]].set_index("Datetime")
    df_Q = df_Q.asfreq("D")

    # Leer el archivo de niveles
    df_WL = pd.read_csv(ruta_nivel, skiprows=14)
    df_WL.columns = ["ISO_UTC", "Local_Timestamp", "Water Level (m)", "Water Level (msnm)"]
    df_WL["ISO_UTC"] = pd.to_datetime(df_WL["ISO_UTC"])
    df_WL["Datetime"] = pd.to_datetime(df_WL["ISO_UTC"]).dt.strftime("%Y-%m-%d")
    df_WL["Datetime"] = pd.to_datetime(df_WL["Datetime"])
    df_WL_rel = df_WL[["Datetime", "Water Level (m)"]].set_index("Datetime")
    df_WL_rel = df_WL_rel.asfreq("D")
    df_WL_abs = df_WL[["Datetime", "Water Level (msnm)"]].set_index("Datetime")
    df_WL_abs.rename(columns={'Water Level (msnm)':'Water Level (m)'}, inplace=True)

    df_WL_abs = df_WL_abs.asfreq("D")

    # Extraer serie de tiempo de caudales
    df_streamflow_filled = df_Q.copy()
    df_streamflow_filled["Streamflow (m3/s)"] = df_streamflow_filled["Streamflow (m3/s)"].fillna(-9999)

    # Extraer serie de tiempo de niveles
    df_waterlevel_filled = df_WL_rel.copy()
    df_waterlevel_filled["Water Level (m)"] = df_waterlevel_filled["Water Level (m)"].fillna(-9999)

    df_waterlevel_abs_filled = df_WL_abs.copy()
    df_waterlevel_abs_filled["Water Level (m)"] = df_waterlevel_abs_filled["Water Level (m)"].fillna(-9999)

    # Guardar en archivos CSV
    df_Q.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/El_Salvador/Datos_Temporales/Observed_Data/Streamflow/{0}.csv".format(code))
    df_WL_rel.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/El_Salvador/Datos_Temporales/Observed_Data/Water_Level/{0}.csv".format(code))
    df_WL_abs.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/El_Salvador/Datos_Temporales/Observed_Data/Water_Level/{0}_WL_abs.csv".format(code))
    df_streamflow_filled.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Central_America/El_Salvador/{0}_Q.csv".format(code))
    df_waterlevel_filled.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Central_America/El_Salvador/{0}_WL.csv".format(code))
    df_waterlevel_abs_filled.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Central_America/El_Salvador/{0}_WL_abs.csv".format(code))
