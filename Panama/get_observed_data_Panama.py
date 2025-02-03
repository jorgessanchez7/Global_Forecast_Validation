import calendar
import numpy as np
import pandas as pd

stations_country = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama_new_Stations.csv")

IDs = stations_country.index.to_list()
Codes = stations_country['samplingFeatureCode'].to_list()
Names = stations_country['name'].to_list()
Latitudes = stations_country['latitude'].to_list()
Longitudes = stations_country['longitude'].to_list()
Elevations = stations_country['elevation_m'].to_list()
Institutions = stations_country['county'].to_list()

# Function to clean numeric values
def clean_value(value):
    if isinstance(value, str):  # Ensure value is a string before processing
        value = value.strip().replace("*", "").strip()  # Remove "*" and extra spaces
        return float(value) if value else np.nan  # Convert to float or NaN if empty
    return np.nan  # If it's not a string, assume it's NaN

# Función para extraer datos de la tabla con estructura definida
def extraer_serie_tiempo(xls, columna_años, columna_dias, columna_inicio, num_meses=12):
    df_list = []

    # Cargar la hoja de Excel (asumimos que es la primera hoja)
    df = xls.parse(sheet_name=0, header=None)

    fila_inicio = 10  # Primera fila con datos
    salto_anios = 34  # Espaciado entre cada nuevo año de datos
    meses = list(range(num_meses))  # 0=Enero, 1=Febrero, ..., 11=Diciembre

    # Iterar sobre los años disponibles
    while True:
        # Extraer el año en la celda correspondiente
        try:
            año = df.iloc[fila_inicio-1, columna_años]
            if pd.isna(año):
                break  # Terminar si no hay más años de datos
            año = int(año)
        except:
            break

        # Extraer días del mes
        dias = df.iloc[fila_inicio-1:fila_inicio + 31, columna_dias].dropna().astype(int).tolist()

        # Extraer valores mes a mes
        for mes_idx, mes in enumerate(meses):
            columna_mes = columna_inicio + mes_idx
            valores = df.iloc[fila_inicio - 1:fila_inicio + 30, columna_mes].astype(str).str.strip()

            # Clean values by removing "*" and converting them to float
            valores = valores.apply(clean_value)

            # Get the last valid day of the month for the given year
            ultimo_dia_mes = calendar.monthrange(año, mes_idx + 1)[1]

            # Filtrar días inválidos (ejemplo: 30 y 31 de febrero)
            fechas = [pd.Timestamp(year=año, month=mes_idx + 1, day=d) for d in dias if d <= ultimo_dia_mes]
            valores_filtrados = valores[:len(fechas)]  # Ajustar valores a la cantidad de fechas válidas
            
            
            # Crear DataFrame temporal con las fechas y valores
            datos = pd.DataFrame({"Datetime": fechas, "Value": valores_filtrados})
            df_list.append(datos)

        # Saltar al siguiente año de datos
        fila_inicio += salto_anios

    # Unir todos los datos y manejar valores faltantes
    df_final = pd.concat(df_list, ignore_index=True)
    df_final = df_final.set_index("Datetime")
    df_final.dropna(inplace=True)
    df_final = df_final.asfreq("D")  # Rellenar serie con todos los días posibles
    return df_final

for id, code, name, latitude, longitude, elevation, institution in zip(IDs, Codes, Names, Latitudes, Longitudes, Elevations, Institutions):

    print(code, ' - ', name, ' - ', latitude, ' - ', longitude, ' - ', elevation, 'm')

    # Ruta del archivo Excel
    ruta_excel = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama/Datos_Temporales/raw_data/{0}.xlsx".format(code)

    # Cargar la hoja de datos
    xls = pd.ExcelFile(ruta_excel)

    # Extraer serie de tiempo de caudales
    df_streamflow = extraer_serie_tiempo(xls, columna_años=0, columna_dias=1, columna_inicio=2)
    df_streamflow.columns = ["Streamflow (m3/s)"]
    df_streamflow_filled = df_streamflow.copy()
    df_streamflow_filled["Streamflow (m3/s)"] = df_streamflow_filled["Streamflow (m3/s)"].fillna(-9999)

    # Extraer serie de tiempo de niveles
    df_waterlevel = extraer_serie_tiempo(xls, columna_años=15, columna_dias=16, columna_inicio=17)
    df_waterlevel.columns = ["Water Level (m)"]
    df_waterlevel_filled = df_waterlevel.copy()
    df_waterlevel_filled["Water Level (m)"] = df_waterlevel_filled["Water Level (m)"].fillna(-9999)

    # Guardar en archivos CSV
    df_streamflow.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama/Datos_Temporales/Observed_Data/Streamflow/{0}.csv".format(code))
    df_waterlevel.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama/Datos_Temporales/Observed_Data/Water_Level/{0}.csv".format(code))
    df_streamflow_filled.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Central_America/Panama/{0}_Q.csv".format(code))
    df_waterlevel_filled.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Central_America/Panama/{0}_WL.csv".format(code))
