import os
import unidecode
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


def assign_state(df):
    df_missing = df[df['state'].isna() & df['country_name'].notna()].copy()
    print(df_missing)
    gdf = gpd.GeoDataFrame(df_missing, geometry=gpd.points_from_xy(df_missing['Longitude'], df_missing['Latitude']), crs="EPSG:4326")

    for i, row in gdf.iterrows():
        country_name = row['country_name']

        print(row['Name'], ' - ', row['Latitude'], ' - ', row['Longitude'], ' - ', country_name)

        point = row.geometry
        country_path = f"/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Countries/{country_name}"

        try:
            state_shp = next((f for f in os.listdir(country_path) if f.endswith('_1.shp')), None)
            if state_shp:
                shp_path = os.path.join(country_path, state_shp)
                state_gdf = gpd.read_file(shp_path).to_crs("EPSG:4326")
                match = state_gdf[state_gdf.contains(point)]
                if not match.empty:
                    state_name = match.iloc[0]['NAME_1']
                    df.at[i, 'state'] = unidecode.unidecode(state_name)
        except Exception as e:
            print(f"[STATE] Error en estación {row['Name']} ({country_name}): {e}")

    return df

stations_metadata = pd.read_csv("hydroweb_metadata_country.csv", index_col=0)
stations_metadata = assign_state(stations_metadata)

stations_metadata.to_csv("hydroweb_metadata_state.csv")