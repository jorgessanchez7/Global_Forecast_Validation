import os
import re
import calendar
import pandas as pd
from datetime import datetime
from unidecode import unidecode

# Valores considerados como datos faltantes
missing_values = {"", "-", "--", "---", "S/D", "#DIV/0!", "#N/A"}

# Meses en español
month_map = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AGO": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12
}

def extract_year_from_filename(filename):
    match = re.search(r"(\d{4})", filename)
    return int(match.group(1)) if match else None

def read_time_series(file_path, sheet_names, value_type):
    year = extract_year_from_filename(os.path.basename(file_path))
    if not year:
        return pd.DataFrame()

    xl = pd.ExcelFile(file_path)
    if len(xl.sheet_names) == 1:
        sheet_name = xl.sheet_names[0]
    else:
        sheet_name = next((s for s in xl.sheet_names if s.upper() in sheet_names), None)
        if not sheet_name:
            return pd.DataFrame()

    df = xl.parse(sheet_name, header=None)

    # Buscar la primera fila que tenga un valor "1" en la columna A
    start_row = None
    for i in range(len(df)):
        val = df.iloc[i, 0]
        try:
            if int(str(val).strip()) == 1:
                start_row = i
                break
        except:
            continue

    if start_row is None or start_row == 0:
        return pd.DataFrame()

    header_row = df.iloc[start_row - 1]
    df.columns = [unidecode(str(c)).strip().upper() for c in header_row]
    df = df.iloc[start_row:]

    # Normalizar nombres de columna
    colnames = df.columns.tolist()
    day_col = next((c for c in colnames if "DIA" in c), None)
    if not day_col:
        return pd.DataFrame()

    records = []
    for _, row in df.iterrows():
        try:
            day = int(row[day_col])
        except:
            continue
        for month_abbr, month in month_map.items():
            if month_abbr in df.columns:
                val = row[month_abbr]
                if pd.isna(val) or str(val).strip() in missing_values:
                    continue
                try:
                    date = datetime(year, month, day)
                    records.append((date, float(val)))
                except:
                    continue  # Fecha inválida

    return pd.DataFrame(records, columns=["Datetime", value_type])

def process_station_data(station_folder):
    all_level_data = []
    all_flow_data = []

    for fname in os.listdir(station_folder):
        if not fname.lower().endswith(('.xls', '.xlsx')):
            continue
        full_path = os.path.join(station_folder, fname)
        lower_fname = fname.lower()
        #print(lower_fname)

        if "nivel" in lower_fname:
            level_df = read_time_series(full_path, {"NIVEL", "RESUMEN"}, "Water Level (m)")
            if level_df.empty:
                print(f"⚠️  Sin datos válidos de NIVELES en {fname}")
            all_level_data.append(level_df)

        elif "caudal" in lower_fname:
            flow_df = read_time_series(full_path, {"CAUDAL"}, "Caudal (m3/s)")
            if flow_df.empty:
                print(f"⚠️  Sin datos válidos de CAUDALES en {fname}")
            all_flow_data.append(flow_df)

        else:
            level_df = read_time_series(full_path, {"NIVEL", "RESUMEN"}, "Water Level (m)")
            flow_df = read_time_series(full_path, {"CAUDAL"}, "Caudal (m3/s)")
            if df_level.empty:
                print(f"⚠️  Sin datos de NIVELES en {fname}")
            if df_flow.empty:
                print(f"⚠️  Sin datos de CAUDALES en {fname}")
            all_level_data.append(level_df)
            all_flow_data.append(flow_df)

    # Filtrar los DataFrames no vacíos antes de concatenar
    level_data_valid = [df for df in all_level_data if not df.empty]
    flow_data_valid = [df for df in all_flow_data if not df.empty]

    # Concatenar solo si hay datos válidos
    if level_data_valid:
        level_ts = pd.concat(level_data_valid).drop_duplicates(subset="Datetime").sort_values("Datetime").reset_index(
            drop=True)
    else:
        level_ts = pd.DataFrame(columns=["Datetime", "Water Level (m)"])

    if flow_data_valid:
        flow_ts = pd.concat(flow_data_valid).drop_duplicates(subset="Datetime").sort_values("Datetime").reset_index(
            drop=True)
    else:
        flow_ts = pd.DataFrame(columns=["Datetime", "Caudal (m3/s)"])

    # ⚠️ Conservar caudales solo donde hay nivel
    #valid_flow = pd.merge(level_ts[["Datetime"]], flow_ts, on="Datetime", how="inner")
    # Combinar nivel y caudal (caudal ya filtrado por fechas con nivel)
    #merged_df = pd.merge(level_ts, valid_flow, on="Datetime", how="left")

    merged_df = pd.merge(level_ts, flow_ts, on="Datetime", how="outer").sort_values("Datetime").reset_index(drop=True)

    #return level_ts, valid_flow, merged_df
    return level_ts, flow_ts, merged_df

#code = '45-24-03'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/Cuenca_45_Rio_Coco/Formato_excel/45--24-03-Coco_Quilali'
#code = '45-03-01'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/Cuenca_45_Rio_Coco/Formato_excel/45-03-01_PANTASMA _EN_ANTIOQUIA'
#code = '55-01-03'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_01_03_GRANDE DE MATAGALPA EN SÉBACO'
#code = '55-01-04'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_01_04_GRANDE DE MATAGALPA EN TRAPICHITO'
#code = '55-01-06'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_01_06_GRANDE DE MATAGALPA EN NICAREY'
#code = '55-01-08'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_01_08'
#code = '55-02-01'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_02_01_TUMA_DORADO_I'
#code = '55-02-02'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_02_02_TUMA_YASICA'
#code = '55-02-03'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_02_03_TUMA_LOS ENCUENTROS'
#code = '55-02-05'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_02_05_TUMA_MASAPA'
#code = '55-02-06'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_02_06_TUMA _JIGUINA'
#code = '55-02-09'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_02_09_TUMA_BOBOKE'
#code = '55-02-13'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_02_13_TUMA_DORADO_II'
#code = '55-03-01'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/CUENCA_55_RIO_GDE_MATAGALPA/Formato Excel/55_03_01_JIGUINA EN JIGUINA'
#code = '66-01-01'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/Cuenca66_Río Tamarindo/Formato Excel/66_01_01_Tamarindo_enTamarindo'
#code = '68-01-02'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/Cuenca68_EntreTamarindoyBrito/Formato Excel/68_01_02_Soledad_Contrabando'
#code = '70-01-01'
#data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/Cuenca70_Río Brito/Formato Excel/70_01_01_Brito_Miramar'
code = '70-01-02'
data_folder = '/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/raw_data/Información Hidrológica/Cuenca70_Río Brito/Formato Excel/70_01_02_Brito_La Flor'

water_level_df, streamflow_df, data_df = process_station_data(data_folder)

data_df = data_df.rename(columns={"Datetime": "Fecha", "Water Level (m)": "Nivel", "Caudal (m3/s)": "Caudal"})
data_df = data_df.set_index("Fecha")
data_df.index = pd.to_datetime(data_df.index)
full_range = pd.date_range(start=data_df.index.min(), end=data_df.index.max(), freq="D")
data_df = data_df.reindex(full_range)

data_df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/GEOGLOWS_Applications/Central_America/Nicaragua/Datos_Temporales/Intermediate_Data/{0}.csv".format(code))