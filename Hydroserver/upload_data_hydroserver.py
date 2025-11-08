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


## Observed Properties

observed_properties = hs_api.observedproperties.list()

print(observed_properties)


for country_id in countries_id:

  workspace_observed_properties = hs_api.observedproperties.list(workspace=country_id)

  print(workspace_observed_properties)


## Units

units = hs_api.units.list()

print(units)


for country_id in countries_id:

  workspace_units = hs_api.units.list(workspace=country_id)

  print(workspace_units)


## Sensors

sensors = hs_api.sensors.list()

print(sensors)


## Processing Levels

processing_levels = hs_api.processinglevels.list()

print(processing_levels)


for country_id in countries_id:

  workspace_processing_levels = hs_api.processinglevels.list(workspace=country_id)

  print(workspace_processing_levels)


## DataStreams

datastreams = hs_api.datastreams.list()

print(datastreams)


for country_id in countries_id:

  workspace_datastreams = hs_api.datastreams.list(workspace=country_id)

  print(workspace_datastreams)


## Adding Observartions

### Discharge

discharge_stations = stations_df.loc[stations_df['Q'] == 'YES']

folders_Q = discharge_stations['Folder'].to_list()
datasources_Q = discharge_stations['Data_Source'].to_list()
uids_Q = discharge_stations['uid'].to_list()
codes_Q = discharge_stations['samplingFeatureCode'].to_list()
names_Q = discharge_stations['name'].to_list()
visibility_Q = discharge_stations['isVisible'].to_list()
workspaces_Q = discharge_stations['country_id'].to_list()

for folder_Q, datasource_Q, uid, code, name, isVisible, workspace_Q in zip(folders_Q, datasources_Q, uids_Q, codes_Q, names_Q, visibility_Q, workspaces_Q):

  print(folder_Q,  ' - ', datasource_Q,  ' - ', uid, ' - ', code, ' - ', name)

  try:

    thing = hs_api.things.get(uid=uid)
    thing_datastreams = thing.datastreams

    for single_datastream in thing_datastreams:
      if single_datastream.name == 'Observed Streamflow':
        datastream = hs_api.datastreams.get(uid=str(single_datastream.uid))

        new_observations = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder_Q, datasource_Q, code), index_col=0)
        new_observations.index = pd.to_datetime(new_observations.index)
        new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
        new_observations.index = pd.to_datetime(new_observations.index)

        new_observations = new_observations.reset_index()

        column_discharge = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "phenomenon_time", column_discharge: "result"})
        new_observations['phenomenon_time'] = pd.to_datetime(new_observations['phenomenon_time']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")

        # Upload the observations to HydroServer
        datastream.load_observations(new_observations)

  except Exception as e:

    print(e.__str__())


### Water Level

waterlevel_stations = stations_df.loc[stations_df['WL'] == 'YES']

folders_WL = waterlevel_stations['Folder'].to_list()
datasources_WL = waterlevel_stations['Data_Source'].to_list()
uids_WL = waterlevel_stations['uid'].to_list()
codes_WL = waterlevel_stations['samplingFeatureCode'].to_list()
names_WL = waterlevel_stations['name'].to_list()
visibility_WL = waterlevel_stations['isVisible'].to_list()
workspaces_WL = waterlevel_stations['country_id'].to_list()

for folder_WL, datasource_WL, uid, code, name, isVisible, workspace_WL in zip(folders_WL, datasources_WL, uids_WL, codes_WL, names_WL, visibility_WL, workspaces_WL):

  print(folder_WL,  ' - ', datasource_WL,  ' - ', uid, ' - ', code, ' - ', name)

  try:

    thing = hs_api.things.get(uid=uid)
    thing_datastreams = thing.datastreams

    for single_datastream in thing_datastreams:
      if single_datastream.name == 'Observed Water Level':
        datastream = hs_api.datastreams.get(uid=str(single_datastream.uid))

        new_observations = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_WL.csv'.format(folder_WL, datasource_WL, code), index_col=0)
        new_observations.index = pd.to_datetime(new_observations.index)
        new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
        new_observations.index = pd.to_datetime(new_observations.index)

        new_observations = new_observations.reset_index()

        column_water_level = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "phenomenon_time", column_water_level: "result"})
        new_observations['phenomenon_time'] = pd.to_datetime(new_observations['phenomenon_time']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")

        # Upload the observations to HydroServer
        datastream.load_observations(new_observations)

  except Exception as e:

    print(e.__str__())

