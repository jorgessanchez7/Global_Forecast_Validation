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

for country in countries:

  new_workspace = hs_api.workspaces.create(
    name=country,
    is_private=False
  )

workspaces = hs_api.workspaces.list()
workspaces_uids = []
workspaces_names = []

for workspace in workspaces.items:
    print(workspace.uid, ' - ', workspace.name)
    workspaces_uids.append(str(workspace.uid))
    workspaces_names.append(workspace.name)

