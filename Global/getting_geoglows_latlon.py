import geoglows
import pandas as pd

df = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison_v0.csv")

# Cargar metadata de GEOGLOWS
metadata_table_url = 'http://geoglows-v2.s3-us-west-2.amazonaws.com/tables/package-metadata-table.parquet'
metadata_table = pd.read_parquet(metadata_table_url)

# Asegurarse de que las columnas estén como float (en caso de que vengan como objetos vacíos)
df['Lat_GEOGLOS_v2'] = pd.to_numeric(df['Lat_GEOGLOS_v2'], errors='coerce')
df['Lon_GEOGLOS_v2'] = pd.to_numeric(df['Lon_GEOGLOS_v2'], errors='coerce')

# Rellenar Lat_GEOGLOS_v2 y Lon_GEOGLOS_v2 a partir de LINKNO == COMID_v2
for i, row in df.iterrows():
    river_id = row['COMID_v2']
    if pd.isna(row['Lat_GEOGLOS_v2']) or pd.isna(row['Lon_GEOGLOS_v2']):
        match = metadata_table[metadata_table['LINKNO'] == river_id]
        if not match.empty:
            df.at[i, 'Lat_GEOGLOS_v2'] = match['lat'].values[0]
            df.at[i, 'Lon_GEOGLOS_v2'] = match['lon'].values[0]

# Si latitude está vacía, copiar desde Lat_GEOGLOS_v2
df['latitude'] = df['latitude'].fillna(df['Lat_GEOGLOS_v2'])
df['longitude'] = df['longitude'].fillna(df['Lon_GEOGLOS_v2'])

# Guardar el CSV actualizado (opcional)
df.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison.csv", index=False)