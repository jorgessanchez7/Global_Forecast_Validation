import os
import re
import pandas as pd

def extract_metadata_from_file(filepath):
    metadata = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith("#"):
                parts = line.strip().split("::")
                if len(parts) == 2:
                    key, value = parts
                    key = key.strip("# ").upper()
                    value = value.strip()
                    metadata[key] = value
    return metadata

def clean_station_name(filename):
    base = filename.replace("hydroprd_R_", "").replace("_exp.txt", "")
    return base.lower().capitalize()

def extract_missions(raw_missions):
    if '-' in raw_missions:
        # Casos con formato S6A-0202
        missions = re.findall(r'[A-Z0-9]+(?=-\d+)', raw_missions)
    else:
        # Aceptar tokens que tengan al menos una letra (evita valores como "0085")
        missions = [token for token in raw_missions.split() if any(c.isalpha() for c in token)]
    return "-".join(sorted(set(missions), key=missions.index))

def convert_slope_mm_per_km_to_m_per_m(slope_str):
    if slope_str.upper() == "NA":
        return None
    try:
        slope_val = float(slope_str)
        return slope_val / 1_000_000
    except:
        return None

def collect_metadata(base_folder):
    results = []

    folder_type_map = {
        "HYDROWEB_RIVERS_OPE": "Operational",
        "HYDROWEB_RIVERS_RESEARCH": "Research"
    }

    for subfolder, station_type in folder_type_map.items():
        folder_path = os.path.join(base_folder, subfolder)
        if not os.path.isdir(folder_path):
            continue

        for filename in os.listdir(folder_path):
            if filename.startswith("hydroprd_R_") and filename.endswith("_exp.txt"):
                filepath = os.path.join(folder_path, filename)
                metadata = extract_metadata_from_file(filepath)

                name = clean_station_name(filename)
                missions_raw = metadata.get("MISSION(S)-TRACK(S)", "")
                missions = extract_missions(missions_raw)
                slope_raw = metadata.get("MEAN SLOPE (MM/KM)", "NA")
                slope_m_per_m = convert_slope_mm_per_km_to_m_per_m(slope_raw)

                # Capitalize relevant fields
                basin = metadata.get("BASIN", "").capitalize()
                river = metadata.get("RIVER", "").capitalize()
                ellipsoid = metadata.get("REFERENCE ELLIPSOID", "")
                status = metadata.get("STATUS", "").capitalize()

                results.append({
                    "ID": f"H-{metadata.get('ID', '')}",  # ID forzado como texto
                    "Name": name,
                    "Basin": basin,
                    "River": river,
                    "Latitude": metadata.get("REFERENCE LATITUDE", ""),
                    "Longitude": metadata.get("REFERENCE LONGITUDE", ""),
                    "Elevation": metadata.get("MEAN ALTITUDE(M.MM)", ""),
                    "Ellipsoid": ellipsoid,
                    "Slope (m/m)": slope_m_per_m,
                    "Missions": missions,
                    "First Date of Data": metadata.get("FIRST DATE IN DATASET", ""),
                    "Last Date of Data": metadata.get("LAST DATE IN DATASET", ""),
                    "Number of Measurements": metadata.get("NUMBER OF MEASUREMENTS IN DATASET", ""),
                    "Status": status,
                    "Type": station_type
                })

    return pd.DataFrame(results)

# ==== USO ====
if __name__ == "__main__":
    base_dir = "hydroweb_download"  # Ajusta esta ruta si tus carpetas están en otro lugar
    df_metadata = collect_metadata(base_dir)
    df_metadata.to_csv("hydroweb_metadata.csv", index=False)
    print("Extracción completada. Se guardó en 'hydroweb_metadata.csv'.")
