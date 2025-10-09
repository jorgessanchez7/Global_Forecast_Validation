import os
import re
import sys
import pandas as pd

# === RUTAS DE ENTRADA / SALIDA ===
STATIONS_CSV = r"G:\My Drive\Personal_Files\Post_Doc\Hydroweb\Hydroweb_Stations_row_SHP.csv"

DIR_OP = r"C:\Users\jsanchez\Documents\github\Global_Forecast_Validation\Hydroweb\hydroweb_download\HYDROWEB_RIVERS_OPE"
DIR_RESEARCH = r"C:\Users\jsanchez\Documents\github\Global_Forecast_Validation\Hydroweb\hydroweb_download\HYDROWEB_RIVERS_RESEARCH"

FALLBACK_DIR = r"E:\GEOGloWS\04_Observed_Satellite_Water_Levels\Hydroweb"
OUTPUT_DIR = r"E:\GEOGloWS\04_Observed_Satellite_Water_Levels\Hydroweb"

# === UTILIDADES BÁSICAS ===
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def build_filename(name: str) -> str:
    """
    Regla literal solicitada:
    'hydroprd_R_' + NAME.upper() + '_exp.txt'
    """
    return f"hydroprd_R_{name.upper()}_exp.txt"

def status_to_dir(status: str) -> str | None:
    """Mapea el Status a su carpeta. Tolerante a mayúsculas/minúsculas y espacios."""
    if status is None:
        return None
    s = str(status).strip().lower()
    if s == "operational":
        return DIR_OP
    if s == "research":
        return DIR_RESEARCH
    # por si vienen variantes con espacios o prefijos/sufijos
    if "operational" in s:
        return DIR_OP
    if "research" in s:
        return DIR_RESEARCH
    return None

# === PARSER DEL TXT HYDROWEB ===
# Línea de datos típica:
# 2019-03-06 15:54   362.51     0.04 :  82.2043  46.3669   310.23   -52.28    -0.15 S3B REP 0735 022 OCOG 2.0
DATE_HEIGHT_REGEX = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}\s+(?P<hgt>[+-]?\d+(?:\.\d+)?)\b"
)

def parse_hydroweb_txt(txt_path: str) -> pd.DataFrame:
    """
    Lee un archivo .txt de Hydroweb y extrae:
      - COL 1: DATE (YYYY-MM-DD)
      - COL 3: ORTHOMETRIC HEIGHT (M)
    Devuelve DataFrame con columnas: Datetime, Water Level (m)
    """
    dates, heights = [], []
    found_header_end = False

    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.rstrip("\n")

            # Detecta la línea separadora de cabecera: una línea hecha solo de '#'
            if not found_header_end:
                stripped = line.strip()
                if stripped and set(stripped) == {"#"}:
                    found_header_end = True
                continue

            if not line.strip() or line.lstrip().startswith("#"):
                continue

            # Primero: regex directa
            m = DATE_HEIGHT_REGEX.match(line.strip())
            if m:
                date = m.group("date")                  # YYYY-MM-DD
                height_str = m.group("hgt")
                try:
                    hval = float(height_str)
                except ValueError:
                    hval = float("nan")
                dates.append(date)
                heights.append(hval)
                continue

            # Respaldo: cortar antes de " : " y tomar tokens 0,1,2
            core = line.split(":", 1)[0]
            parts = core.split()
            if len(parts) >= 3 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[0]):
                date = parts[0]
                try:
                    hval = float(parts[2])
                except ValueError:
                    hval = float("nan")
                dates.append(date)
                heights.append(hval)

    df = pd.DataFrame({"Datetime": dates, "Water Level (m)": heights})

    # Limpieza: valores sentinela si aparecieran (no siempre)
    df.replace({9999.0: pd.NA, 9999.999: pd.NA}, inplace=True)
    df = df.dropna(subset=["Water Level (m)"])

    # Fecha en formato YYYY-MM-DD (string)
    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["Datetime"])
    return df

def read_fallback_csv(station_id: str) -> pd.DataFrame | None:
    """
    Lee CSV fallback (ya formateado) si existe: '{FALLBACK_DIR}\\{ID}.csv'
    Debe venir con columnas 'Datetime' y 'Water Level (m)' (o equivalentes comunes).
    """
    path = os.path.join(FALLBACK_DIR, f"{station_id}.csv")
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path, encoding="utf-8")
    # Intento mapear nombres comunes por si vinieran con ligeras variaciones
    lc = {c.lower().strip(): c for c in df.columns}

    dt_col = None
    wl_col = None
    for c in df.columns:
        k = c.lower().strip()
        if k in ("datetime", "date", "fecha"):
            dt_col = c if dt_col is None else dt_col
        if k in ("water level (m)", "water_level_(m)", "water_level", "nivel", "nivel (m)"):
            wl_col = c if wl_col is None else wl_col

    if dt_col is None and "Datetime" in df.columns:
        dt_col = "Datetime"
    if wl_col is None and "Water Level (m)" in df.columns:
        wl_col = "Water Level (m)"

    if dt_col is None or wl_col is None:
        raise ValueError(f"Fallback CSV {path} no tiene columnas esperadas.")

    out = df[[dt_col, wl_col]].copy()
    out.columns = ["Datetime", "Water Level (m)"]
    out["Datetime"] = pd.to_datetime(out["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["Datetime", "Water Level (m)"])
    return out

def write_output_csv(station_id: str, df: pd.DataFrame):
    ensure_dir(OUTPUT_DIR)
    out_path = os.path.join(OUTPUT_DIR, f"{station_id}.csv")
    # Asegura formato final
    out = df.copy()
    out["Datetime"] = pd.to_datetime(out["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["Datetime", "Water Level (m)"])
    out[["Datetime", "Water Level (m)"]].to_csv(out_path, index=False, encoding="utf-8")

def main():
    # Verifica CSV de estaciones
    if not os.path.exists(STATIONS_CSV):
        print(f"[ERROR] No se encuentra el archivo de estaciones: {STATIONS_CSV}")
        sys.exit(1)

    stations = pd.read_csv(STATIONS_CSV, encoding="utf-8")
    required = {"ID", "Name", "Status"}
    if not required.issubset(set(stations.columns)):
        print(f"[ERROR] Faltan columnas en el CSV de estaciones. Se requieren: {required}")
        sys.exit(1)

    total = len(stations)
    ok_txt = 0
    ok_fallback = 0
    skipped = 0
    errors: list[str] = []

    for i, row in stations.iterrows():
        station_id = str(row["ID"]).strip() if pd.notna(row["ID"]) else ""
        name = str(row["Name"]).strip() if pd.notna(row["Name"]) else ""
        status = str(row["Status"]).strip() if pd.notna(row["Status"]) else ""

        if not station_id or not name:
            skipped += 1
            errors.append(f"[{i}] ID o Name vacío. ID='{station_id}', Name='{name}'")
            continue

        try:
            src_dir = status_to_dir(status)
            if src_dir is None:
                # Sin carpeta definida → intenta fallback directo
                fb = read_fallback_csv(station_id)
                if fb is not None and not fb.empty:
                    write_output_csv(station_id, fb)
                    ok_fallback += 1
                    continue
                skipped += 1
                errors.append(f"[{i}] Status no reconocido '{status}' para ID={station_id}")
                continue

            fname = build_filename(name)  # EXACTO: hydroprd_R_{NAME.upper()}_exp.txt
            txt_path = os.path.join(src_dir, fname)

            if os.path.exists(txt_path):
                df = parse_hydroweb_txt(txt_path)
                if not df.empty:
                    write_output_csv(station_id, df)
                    ok_txt += 1
                    continue
                else:
                    # Si el TXT no tiene datos válidos, intenta fallback
                    fb = read_fallback_csv(station_id)
                    if fb is not None and not fb.empty:
                        write_output_csv(station_id, fb)
                        ok_fallback += 1
                        continue
                    skipped += 1
                    errors.append(f"[{i}] TXT sin datos válidos y sin fallback. ID={station_id}, NAME={name}")
                    continue

            # Si no existe el TXT → intenta fallback
            fb = read_fallback_csv(station_id)
            if fb is not None and not fb.empty:
                write_output_csv(station_id, fb)
                ok_fallback += 1
            else:
                skipped += 1
                errors.append(f"[{i}] No se encontró TXT ni fallback CSV para ID={station_id}, NAME={name}, STATUS={status}. Buscado: {txt_path}")

        except Exception as e:
            skipped += 1
            errors.append(f"[{i}] Error con ID={station_id}, NAME={name}: {e}")

    # Resumen
    print("==== RESUMEN ====")
    print(f"Estaciones totales: {total}")
    print(f"OK desde TXT: {ok_txt}")
    print(f"OK desde fallback CSV: {ok_fallback}")
    print(f"Omitidas / con error: {skipped}")
    if errors:
        print("\n-- Detalle de errores/omisiones --")
        for msg in errors[:200]:  # evita imprimir demasiado si hay miles
            print(msg)

if __name__ == "__main__":
    main()
