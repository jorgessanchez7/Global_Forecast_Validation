import re
import os
import pandas as pd
from datetime import datetime

def listar_archivos_excel(carpeta):
    archivos_excel = [f for f in os.listdir(carpeta)  if os.path.isfile(os.path.join(carpeta, f)) and f.lower().endswith(('.xlsx', '.xls'))]
    return archivos_excel

def listar_hojas_excel(ruta_archivo):
    xls = pd.ExcelFile(ruta_archivo)
    return xls.sheet_names

def interpretar_mes_anio(texto):
    """
    Intenta interpretar una cadena que contiene un mes y un año, en varios formatos.
    Ej: 'Jan1981', 'Jan 1981', 'January1981', 'January 1981'
    """
    if not isinstance(texto, str):
        return None

    texto = texto.strip()

    # Buscar patrón de mes y año juntos
    match = re.match(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s?(\d{4})', texto, re.IGNORECASE)
    if match:
        mes_str = match.group(1)
        anio = match.group(2)
        try:
            fecha = datetime.strptime(f"{mes_str} {anio}", "%b %Y")
            return fecha
        except:
            return None
    return None

def es_columna_valida(x):
    if pd.isna(x):
        return False
    x_str = str(x).strip().lower()

    # Palabras clave a eliminar
    palabras_invalidas = ['mean', 'max.', 'min.']

    if x_str == 'date':
        return True

    try:
        pd.to_datetime(x_str)
        return True
    except:
        if x_str == '12 noon':
            return True

    if any(p in x_str for p in palabras_invalidas):
        return False

    return True

def procesar_hoja(ruta_archivo, nombre_hoja):
    # Leer toda la hoja
    df_raw = pd.read_excel(ruta_archivo, sheet_name=nombre_hoja, header=None)

    # Buscar la fila que contiene la palabra 'Date' en la primera columna
    fila_header_idx = None
    for i, val in enumerate(df_raw.iloc[:, 0]):
        if str(val).strip().lower() == 'date':
            fila_header_idx = i
            break

    if fila_header_idx is not None:
        nombres_columnas = df_raw.iloc[fila_header_idx].tolist()
        df = df_raw.iloc[fila_header_idx + 1:].copy()
        df.columns = nombres_columnas
        df = df[df['Date'].notna()].reset_index(drop=True)
        df = df.loc[:, df.columns.notna()]
        df = df.loc[:, df.columns.map(es_columna_valida)]
        df = df.reset_index(drop=True)

    elif fila_header_idx is None:
        print(f"No se encontró fila con encabezado 'Date' en la hoja {nombre_hoja}")
        return []

    # Obtener año y mes desde alguna celda (en este caso, fila 4, columna 5)
    # Esto puede variar; si ya tienes el mes/año en el nombre de la hoja, también podemos usarlo.
    fecha_base = interpretar_mes_anio(nombre_hoja)
    if fecha_base is None:
        print(f"No se pudo interpretar la fecha base en el nombre de la hoja: {nombre_hoja}")
        return []

    registros = []
    for _, fila in df.iterrows():
        dia = fila.iloc[0]
        if pd.isna(dia):
            continue
        try:
            dia_int = int(dia)
        except:
            continue

        for col in df.columns[1:]:
            hora_str = str(col).strip()
            if pd.isna(hora_str) or hora_str.lower() == 'nan':
                continue
            try:
                hora = pd.to_datetime(hora_str).time()
            except:
                if hora_str.lower() == "12 noon":
                    hora = datetime.strptime("12:00 PM", "%I:%M %p").time()
                else:
                    try:
                        hora = datetime.strptime(hora_str, "%I:%M:%S %p").time()
                    except:
                        continue

            valor = fila[col]
            #if pd.isna(valor):
            #    continue
            try:
                fecha_completa = datetime(fecha_base.year, fecha_base.month, dia_int, hora.hour, hora.minute, hora.second)
                registros.append((fecha_completa.strftime("%Y-%m-%d %H:%M:%S"), valor))
            except:
                continue

    return registros

# Ejemplo de uso

#code = '8901'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8901 Double Run/Stage'
#code = '8906'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8906 Benque Viejo/Stage'
#code = '8907'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8907 San Ignacio/Stage'
#code = '8913'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8913 Kendal/Stage'
#code = '8915'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8915 Blue Creek South/Stage'
#code = '8917'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8917 Jordan/Stage'
#code = '8920'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8920 Blue Creek North/Stage'
#code = '8921'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8921 Douglas/Stage'
#code = '8922'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8922 Caledonia/Stage'
#code = '8927'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8927 San Pedro Columbia/Stage'
#code = '8928'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8928 Crooked Tree/Stage'
#code = '8930'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8930 Gales Point/Stage'
#code = '8931'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8931 Tower Hill/Stage'
#code = '8933'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8933 Swasey/Stage'
#code = '8934'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8934 Bladen/Stage'
#code = '8985'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8985 San Antonio/Stage'
#code = '8987'
#carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8987 Santa Cruz/Stage'
code = '8990'
carpeta = r'/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Belize/Time_Series/8990 Crique Sarco/Stage'

archivos = listar_archivos_excel(carpeta)
archivos = sorted(archivos)

serie_total = []

for archivo in archivos:
    print(archivo)

    ruta = carpeta + "/" + archivo

    hojas = listar_hojas_excel(ruta)

    for hoja in hojas:
        print(hoja)

        registros = procesar_hoja(ruta, hoja)
        serie_total.extend(registros)

df_serie_total = pd.DataFrame(serie_total, columns=['Datetime', 'Water Level (m)'])
df_serie_total['Datetime'] = pd.to_datetime(df_serie_total['Datetime'])
df_serie_total = df_serie_total.set_index('Datetime')
df_serie_total.dropna(inplace=True)
df_serie_total = df_serie_total.sort_index()
df_serie_total.index = pd.to_datetime(df_serie_total.index)

rango_fechas = pd.date_range(start=df_serie_total.index.min().date(), end=df_serie_total.index.max().date(), freq='D')
fechas_necesarias = []
for fecha in rango_fechas:
    fechas_necesarias.append(fecha + pd.Timedelta(hours=6))   # 6:00 AM
    fechas_necesarias.append(fecha + pd.Timedelta(hours=18))  # 6:00 PM

fechas_necesarias_idx = pd.DatetimeIndex(fechas_necesarias)
faltantes = fechas_necesarias_idx.difference(df_serie_total.index)
nuevo_indice = df_serie_total.index.union(faltantes)
df_serie_total = df_serie_total.reindex(nuevo_indice).sort_index()

df_serie_total.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Belize/Datos_Temporales/Observed_Data/Water_Level/{}_WL_RT.csv".format(code))

df_diario = df_serie_total.resample('D').mean()
rango_completo = pd.date_range(start=df_diario.index.min(), end=df_diario.index.max(), freq='D')
df_diario = df_diario.reindex(rango_completo)
df_diario.index.name = 'Datetime'

df_serie_total = df_serie_total.fillna(-9999)
df_serie_total.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Belize/Datos_Temporales/Data/{}_WL_RT.csv".format(code))

df_diario.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Belize/Datos_Temporales/Observed_Data/Water_Level/{}_WL.csv".format(code))

df_diario = df_diario.fillna(-9999)
df_diario.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Belize/Datos_Temporales/Data/{}_WL.csv".format(code))