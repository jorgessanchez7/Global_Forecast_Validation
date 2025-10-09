import os
import pandas as pd
import geopandas as gpd
from pathlib import Path

# =========================
# RUTAS
# =========================
g_shp = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\CNE_IDEAM\CNE_IDEAM.shp")
dir_q_2022 = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\Caudales_2022")
dir_wl_2022 = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\NIVELES_2022")

dir_q_out_all = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\Q_data")
dir_q_out_swot = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\Q_data_SWOT")

dir_wl_out_all = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\WL_data")
dir_wl_out_swot = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\WL_data_SWOT")

out_shp_dir = Path(r"G:\My Drive\Personal_Files\SWOT_Collaboration\Colombia\IDEAM\CNE_IDEAM")
out_shp = out_shp_dir / "CNE_HIDROLOGICAS_IDEAM.shp"

# Crear carpetas de salida si no existen
for d in [dir_q_out_all, dir_q_out_swot, dir_wl_out_all, dir_wl_out_swot, out_shp_dir]:
    d.mkdir(parents=True, exist_ok=True)

# =========================
# Funciones auxiliares
# =========================
def read_pipe_data(file_path: Path) -> pd.DataFrame:
    """
    Lee archivo .data con formato:
    Fecha|Valor
    1977-01-01 00:00:00|67.8
    ...
    Devuelve DataFrame con columnas: Datetime (datetime64), Value (float o str si no parsea).
    """
    # Leer con separador '|', saltando líneas vacías
    df = pd.read_csv(
        file_path,
        sep="|",
        header=0,
        names=["Fecha", "Valor"],
        dtype={"Fecha": "string", "Valor": "string"},
        engine="python"
    )
    # Limpieza básica
    df = df.dropna(how="all")
    # Parseo de fecha
    df["Datetime"] = pd.to_datetime(df["Fecha"].str.strip(), errors="coerce")
    # Mantener el valor tal cual (pero quitando espacios)
    df["Value"] = df["Valor"].str.strip()
    # Quitar filas sin fecha válida
    df = df[df["Datetime"].notna()].copy()
    # Ordenar por fecha por si acaso
    df = df.sort_values("Datetime")
    return df[["Datetime", "Value"]]

def write_q_csvs(code: str, src_file: Path):
    df = read_pipe_data(src_file)
    # CSV completo
    out_all = dir_q_out_all / f"{code}_Q.csv"
    df_out = df.copy()
    # Formato de fecha YYYY-MM-DD (sin hora)
    df_out["Datetime"] = df_out["Datetime"].dt.strftime("%Y-%m-%d")
    df_out = df_out.rename(columns={"Value": "Streamflow (m3/s)"})
    df_out.to_csv(out_all, index=False)

    # CSV desde 2022-01-01
    df_swot = df[df["Datetime"] >= pd.Timestamp("2022-01-01")].copy()
    df_swot["Datetime"] = df_swot["Datetime"].dt.strftime("%Y-%m-%d")
    df_swot = df_swot.rename(columns={"Value": "Streamflow (m3/s)"})
    out_swot = dir_q_out_swot / f"{code}_Q.csv"
    df_swot.to_csv(out_swot, index=False)

def write_wl_csvs(code: str, src_file: Path):
    df = read_pipe_data(src_file)
    # CSV completo
    out_all = dir_wl_out_all / f"{code}_WL.csv"
    df_out = df.copy()
    df_out["Datetime"] = df_out["Datetime"].dt.strftime("%Y-%m-%d")
    df_out = df_out.rename(columns={"Value": "Water Level (cm)"})
    df_out.to_csv(out_all, index=False)

    # CSV desde 2022-01-01
    df_swot = df[df["Datetime"] >= pd.Timestamp("2022-01-01")].copy()
    df_swot["Datetime"] = df_swot["Datetime"].dt.strftime("%Y-%m-%d")
    df_swot = df_swot.rename(columns={"Value": "Water Level (cm)"})
    out_swot = dir_wl_out_swot / f"{code}_WL.csv"
    df_swot.to_csv(out_swot, index=False)

# =========================
# Flujo principal
# =========================
# Leer shapefile
gdf = gpd.read_file(g_shp)

# Asegurar que CODIGO exista y sea texto
if "CODIGO" not in gdf.columns:
    raise ValueError("No se encontró el campo 'CODIGO' en el shapefile original.")

gdf["CODIGO"] = gdf["CODIGO"].astype(str).str.strip()

# Preparar columnas Caudal / Nivel
for col in ["Caudal", "Nivel"]:
    if col not in gdf.columns:
        gdf[col] = None  # se guardará como Null en DBF

# Procesar por CODIGO
for idx, row in gdf.iterrows():
    code = row["CODIGO"]

    # ---- Caudal
    q_file = dir_q_2022 / f"Q_MEDIA_D@{code}.data"
    if q_file.exists() and q_file.is_file():
        gdf.at[idx, "Caudal"] = "SI"
        try:
            write_q_csvs(code, q_file)
        except Exception as e:
            # Si falla la escritura, marcamos igualmente que existe el archivo,
            # pero avisamos por consola
            print(f"[WARN] Falló escritura CSV Caudal para {code}: {e}")
    else:
        gdf.at[idx, "Caudal"] = "NO"

    # ---- Nivel
    wl_file = dir_wl_2022 / f"NV_MEDIA_D@{code}.data"
    if wl_file.exists() and wl_file.is_file():
        gdf.at[idx, "Nivel"] = "SI"
        try:
            write_wl_csvs(code, wl_file)
        except Exception as e:
            print(f"[WARN] Falló escritura CSV Nivel para {code}: {e}")
    else:
        gdf.at[idx, "Nivel"] = "NO"

# Guardar shapefile enriquecido (todas las estaciones)
# (Si quieres sobrescribir el original con columnas nuevas, descomenta):
# gdf.to_file(g_shp, driver="ESRI Shapefile", encoding="utf-8")

# Seleccionar estaciones con al menos una variable disponible
gdf_sel = gdf[(gdf["Caudal"] == "SI") | (gdf["Nivel"] == "SI")].copy()

# Guardar shapefile filtrado final
gdf_sel.to_file(out_shp, driver="ESRI Shapefile", encoding="utf-8")

print("Listo. Se actualizaron columnas Caudal/Nivel, se generaron CSVs y se guardó el shapefile filtrado.")
