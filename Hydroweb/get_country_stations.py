import pycountry
import pandas as pd
import reverse_geocoder as rg

def complete_country_info(df):
    # Asegurar que las columnas existen
    if 'country_name' not in df.columns:
        df['country_name'] = None
    if 'country' not in df.columns:
        df['country'] = None

    for i, row in df.iterrows():
        # Solo si faltan ambos campos o alguno
        if pd.isna(row['country_name']) or pd.isna(row['country']):
            try:
                lat = float(row['Latitude'])
                lon = float(row['Longitude'])
                location = rg.search((lat, lon), mode=1)[0]  # devuelve un dict
                country_name = location['cc']  # código ISO-2
                country_obj = pycountry.countries.get(alpha_2=country_name)

                if country_obj:
                    if pd.isna(row['country_name']):
                        df.at[i, 'country_name'] = country_obj.name
                    if pd.isna(row['country']):
                        df.at[i, 'country'] = country_obj.alpha_2
            except Exception as e:
                print(f"Error geolocalizando fila {i} con lat={row['Latitude']}, lon={row['Longitude']}: {e}")

    return df

stations_metadata = pd.read_csv("hydroweb_metadata.csv", index_col=0)
stations_metadata = complete_country_info(stations_metadata)

stations_metadata.to_csv("hydroweb_metadata_country.csv")