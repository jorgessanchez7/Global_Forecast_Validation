import pandas as pd

def leer_csv_con_formato(ruta_archivo):
    # Detectar la fila del encabezado
    with open(ruta_archivo, 'r', encoding='utf-8') as f:
        lineas = f.readlines()

    header_row = None
    for i, linea in enumerate(lineas[:5]):
        if 'Date' in linea or 'Fecha' in linea:
            header_row = i
            break
    if header_row is None:
        header_row = 0

    # Leer el archivo
    df = pd.read_csv(ruta_archivo, header=header_row)

    # Asignar columnas
    col_fecha = df.columns[0]
    col_variable = df.columns[1]

    # Convertir fechas
    df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce')

    # Crear serie completa de 15 minutos
    inicio, fin = df[col_fecha].min(), df[col_fecha].max()
    rango_15min = pd.date_range(start=inicio, end=fin, freq='15min')
    df_15min = df.set_index(col_fecha).reindex(rango_15min).reset_index()
    df_15min.rename(columns={'index': col_fecha}, inplace=True)
    df_15min[col_variable] = df_15min[col_variable].fillna(-9999)

    # Guardar serie de 15 minutos
    nombre_15min = ruta_archivo.replace('.csv', '_15min.csv')
    df_15min.to_csv(nombre_15min, index=False)
    print(f'Serie de 15 minutos guardada como: {nombre_15min}')

    # Crear serie diaria
    df_diaria = df.groupby(df[col_fecha].dt.date)[col_variable].mean().reset_index()
    df_diaria.columns = [col_fecha, col_variable]
    df_diaria[col_fecha] = pd.to_datetime(df_diaria[col_fecha])
    rango_diario = pd.date_range(start=inicio.date(), end=fin.date(), freq='D')
    df_diaria = df_diaria.set_index(col_fecha).reindex(rango_diario).reset_index()
    df_diaria.rename(columns={'index': col_fecha}, inplace=True)
    df_diaria[col_variable] = df_diaria[col_variable].fillna(-9999)

    # Guardar serie diaria
    nombre_diaria = ruta_archivo.replace('.csv', '_diaria.csv')
    df_diaria.to_csv(nombre_diaria, index=False)
    print(f'Serie diaria guardada como: {nombre_diaria}')


# Ejecutar el procesamiento en la carpeta especificada

stations = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras_new_Stations.csv")

stations = stations.loc[stations['Q_15min'] == 'YES']

ids = stations['samplingFeatureCode'].to_list()
names = stations['name'].to_list()
rivers = stations['River'].to_list()
countries = stations['country_name'].to_list()

#
for id, name, river, country in zip(ids, names, rivers, countries):

    print(id, ' - ',  name, ' - ', river, ' - ', country)

    # Solicitar la ruta del archivo al usuario
    ruta_archivo = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/raw_data/{}_Q_RT.csv".format(id)
    try:
        leer_csv_con_formato(ruta_archivo)
    except Exception as e:
        print(f'Error procesando el archivo: {e}')

#
stations = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras_new_Stations.csv")

stations = stations.loc[stations['Q_15min_qq'] == 'YES']

ids = stations['samplingFeatureCode'].to_list()
names = stations['name'].to_list()
rivers = stations['River'].to_list()
countries = stations['country_name'].to_list()

for id, name, river, country in zip(ids, names, rivers, countries):

    print(id, ' - ',  name, ' - ', river, ' - ', country, ' - ')

    # Solicitar la ruta del archivo al usuario
    ruta_archivo = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/raw_data/{}_Q_RT_qq.csv".format(id)
    try:
        leer_csv_con_formato(ruta_archivo)
    except Exception as e:
        print(f'Error procesando el archivo: {e}')



stations = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras_new_Stations.csv")

stations = stations.loc[stations['WL_15min'] == 'YES']

ids = stations['samplingFeatureCode'].to_list()
names = stations['name'].to_list()
rivers = stations['River'].to_list()
countries = stations['country_name'].to_list()

#
for id, name, river, country in zip(ids, names, rivers, countries):

    print(id, ' - ',  name, ' - ', river, ' - ', country)

    # Solicitar la ruta del archivo al usuario
    ruta_archivo = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/raw_data/{}_WL_RT.csv".format(id)
    try:
        leer_csv_con_formato(ruta_archivo)
    except Exception as e:
        print(f'Error procesando el archivo: {e}')