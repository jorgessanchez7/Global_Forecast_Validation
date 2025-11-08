import folium
import getpass
import requests
import pandas as pd
from scipy import stats
from hydroserverpy import HydroServer
from folium.plugins import MarkerCluster

## Get your credentials

host = 'https://hydroserver.geoglows.org'

username = input("Enter your email: ")
password = getpass.getpass("Enter your password: ")

hs_api = HydroServer(host=host, email=username, password=password)


## Get Some Data!

stations_df = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\World_Stations_Hydroserver.csv', keep_default_na=False)


## Workspaces!

countries = stations_df['country_name'].unique().tolist()

workspaces = hs_api.workspaces.list()
workspaces_uids = []
workspaces_names = []

for workspace in workspaces.items:
    print(workspace.uid, ' - ', workspace.name)
    workspaces_uids.append(str(workspace.uid))
    workspaces_names.append(workspace.name)


## Things

countries_id = stations_df['country_id'].unique().tolist()

for country_id in countries_id:

  stations_df_country = stations_df[stations_df['country_id'] == country_id]

  # Iterate over the sampled stations and upload them
  uploaded_stations = []
  for i, row in stations_df_country.iterrows():
    print(f"Uploading station {i+1}/{len(stations_df)}: {row['name']}- {row['country_name']} - {country_id}")
    new_thing = hs_api.things.create(
        name=row['name'],
        description=row['description'],
        sampling_feature_type=str(row['COMID_v2']),
        sampling_feature_code=str(row['samplingFeatureCode']),
        site_type=row['siteType'],
        latitude=str(row['latitude']),
        longitude=str(row['longitude']),
        elevation_m=str(row['elevation_m']),
        elevation_datum=str(row['elevationDatum']),
        state=row['state'],
        county=row['county'],
        country=row['country'],
        data_disclaimer=row['dataDisclaimer'],
        is_private=False,
        workspace=country_id
    )
    uploaded_stations.append(new_thing)