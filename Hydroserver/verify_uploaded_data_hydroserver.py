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


## Verify Uploaded Data

### Discharge

discharge_stations = stations_df.loc[stations_df['Q'] == 'YES']

folders_Q = discharge_stations['Folder'].to_list()
datasources_Q = discharge_stations['Data_Source'].to_list()
uids_Q = discharge_stations['uid'].to_list()
codes_Q = discharge_stations['samplingFeatureCode'].to_list()
names_Q = discharge_stations['name'].to_list()
visibility_Q = discharge_stations['isVisible'].to_list()
workspaces_Q = discharge_stations['country_id'].to_list()

no_discharge_stations = pd.DataFrame()

for folder_Q, datasource_Q, uid, code, name, isVisible, workspace_Q in zip(folders_Q, datasources_Q, uids_Q, codes_Q, names_Q, visibility_Q, workspaces_Q):

  print(folder_Q,  ' - ', datasource_Q,  ' - ', uid, ' - ', code, ' - ', name)

  thing = hs_api.things.get(uid=uid)
  thing_datastreams = thing.datastreams

  for single_datastream in thing_datastreams:
    if single_datastream.name == 'Observed Streamflow':

      new_observations = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder_Q, datasource_Q, code), index_col=0)
      new_observations.index = pd.to_datetime(new_observations.index)
      new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
      new_observations.index = pd.to_datetime(new_observations.index)

      #print(new_observations)

      obs_Q = hs_api.datastreams.get_observations(uid=str(single_datastream.uid), fetch_all = True)
      obs_Q = obs_Q.dataframe
      obs_Q.set_index('phenomenon_time', inplace=True)
      obs_Q.index = pd.to_datetime(obs_Q.index)
      obs_Q.index = obs_Q.index.to_series().dt.strftime("%Y-%m-%d")
      obs_Q.index = pd.to_datetime(obs_Q.index)
      try:
        obs_Q.drop(columns=['result_qualifier_codes'], inplace=True)
      except Exception as e:
        print(e.__str__())
      #print(obs_Q)


      # Compare the index values
      index_values_equal = new_observations.index.equals(obs_Q.index)
      #print(f"Index values are equal: {index_values_equal}")

      # Compare the column values with a numerical tolerance
      # Assuming the first column of new_observations contains the values to compare
      column_values_equal = new_observations.iloc[:, 0].equals(obs_Q['result'])

      if index_values_equal:
        print(' ')
        if column_values_equal:
          print(" ")
        else:
          pd.testing.assert_series_equal(new_observations.iloc[:, 0], obs_Q['result'], check_dtype=False, rtol=1e-2, atol=1e-3)
      else:

        station_data = pd.DataFrame({
          'Folder': [folder_Q],
          'Data_Source': [datasource_Q],
          'Code': [code],
          'Name': [name],
          'uid': [uid],
          'workspace': [workspace_Q],
        })

        no_discharge_stations = pd.concat([no_discharge_stations, station_data], ignore_index=True)

no_discharge_stations.to_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\No_Discharge_Stations_World.csv')


### Water Level

waterlevel_stations = stations_df.loc[stations_df['WL'] == 'YES']

folders_WL = waterlevel_stations['Folder'].to_list()
datasources_WL = waterlevel_stations['Data_Source'].to_list()
uids_WL = waterlevel_stations['uid'].to_list()
codes_WL = waterlevel_stations['samplingFeatureCode'].to_list()
names_WL = waterlevel_stations['name'].to_list()
visibility_WL = waterlevel_stations['isVisible'].to_list()
workspaces_WL = waterlevel_stations['country_id'].to_list()

no_water_level_stations = pd.DataFrame()

for folder_WL, datasource_WL, uid, code, name, isVisible, workspace_WL in zip(folders_WL, datasources_WL, uids_WL, codes_WL, names_WL, visibility_WL, workspaces_WL):

  print(folder_WL,  ' - ', datasource_WL,  ' - ', uid, ' - ', code, ' - ', name)

  thing = hs_api.things.get(uid=uid)
  thing_datastreams = thing.datastreams

  for single_datastream in thing_datastreams:
    if single_datastream.name == 'Observed Water Level':

      new_observations = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_WL.csv'.format(folder_WL, datasource_WL, code), index_col=0)
      new_observations.index = pd.to_datetime(new_observations.index)
      new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
      new_observations.index = pd.to_datetime(new_observations.index)

      #print(new_observations)

      obs_WL = hs_api.datastreams.get_observations(uid=str(single_datastream.uid), fetch_all = True)
      obs_WL = obs_WL.dataframe
      obs_WL.set_index('phenomenon_time', inplace=True)
      obs_WL.index = pd.to_datetime(obs_WL.index)
      obs_WL.index = obs_WL.index.to_series().dt.strftime("%Y-%m-%d")
      obs_WL.index = pd.to_datetime(obs_WL.index)
      try:
        obs_WL.drop(columns=['result_qualifier_codes'], inplace=True)
      except Exception as e:
        print(e.__str__())
      #print(obs_WL)


      # Compare the index values
      index_values_equal = new_observations.index.equals(obs_WL.index)
      #print(f"Index values are equal: {index_values_equal}")

      # Compare the column values with a numerical tolerance
      # Assuming the first column of new_observations contains the values to compare
      column_values_equal = new_observations.iloc[:, 0].equals(obs_WL['result'])

      if index_values_equal:
        print(' ')
        if column_values_equal:
          print(" ")
        else:
          pd.testing.assert_series_equal(new_observations.iloc[:, 0], obs_WL['result'], check_dtype=False, rtol=1e-2, atol=1e-3)
      else:

        station_data = pd.DataFrame({
          'Folder': [folder_WL],
          'Data_Source': [datasource_WL],
          'Code': [code],
          'Name': [name],
          'uid': [uid],
          'workspace': [workspace_WL],
        })

        no_water_level_stations = pd.concat([no_water_level_stations, station_data], ignore_index=True)

no_water_level_stations.to_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\No_Water_Level_Stations_World.csv')

