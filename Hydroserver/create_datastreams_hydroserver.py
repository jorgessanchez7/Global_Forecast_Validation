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

  workspace_observed_properties = hs_api.observedproperties.list(workspace=workspace_Q)

  for observed_property in workspace_observed_properties.items:
    if observed_property.name == 'Water level':
        new_observed_property_WL = observed_property
    elif observed_property.name == 'Streamflow':
        new_observed_property_Q = observed_property

  workspace_units = hs_api.units.list(workspace=workspace_Q)

  for unit in workspace_units.items:
    if unit.name == 'Cubic Meter per Second':
        new_unit_Q = unit
    elif unit.name == 'Meter':
        new_unit_WL_m = unit
    elif unit.name == 'Centimeter':
        new_unit_WL_cm = unit

  workspace_sensors = hs_api.sensors.list(workspace=workspace_Q)

  for sensor in workspace_sensors.items:
    if sensor.name == 'Field Observation':
        new_sensor_obs = sensor

  workspace_processing_levels = hs_api.processinglevels.list(workspace=workspace_Q)

  for processing_level in workspace_processing_levels.items:
    if processing_level.code == '-9999':
        new_processing_level_unknown = processing_level

  isPrivate = not isVisible

  print(folder_Q,  ' - ', datasource_Q,  ' - ', uid, ' - ', code, ' - ', name, ' - ', isVisible, ' - ', isPrivate, ' - ', workspace_Q, ' - ', str(new_observed_property_Q.uid), ' - ', str(new_unit_Q.uid), ' - ', str(new_sensor_obs.uid), ' - ', str(new_processing_level_unknown.uid))

  new_observations = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder_Q, datasource_Q, code), index_col=0)
  new_observations.index = pd.to_datetime(new_observations.index)
  new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
  new_observations.index = pd.to_datetime(new_observations.index)

  time_diff_days = new_observations.index.to_series().diff() / pd.Timedelta(days=1)
  mode_diff = stats.mode(time_diff_days.dropna())
  most_frequent_diff = mode_diff.mode

  print(f"Uploading Datastream")

  new_datastream = hs_api.datastreams.create(
    name='Observed Streamflow',
    description='Observed Streamflow Values for {0} - {1}'.format(code, name),
    observation_type='OM_Measurement',
    sampled_medium='Liquid aqueous',
    no_data_value=-9999,
    aggregation_statistic='Continuous',
    time_aggregation_interval=1,
    status='Ongoing',
    result_type='Timeseries',
    value_count=0,
    phenomenon_begin_time=new_observations.index[0],
    phenomenon_end_time=None,
    result_begin_time=new_observations.index[0],
    result_end_time=None,
    is_visible=isVisible,
    is_private=False,
    thing=str(uid),
    sensor=str(new_sensor_obs.uid),
    observed_property=str(new_observed_property_Q.uid),
    processing_level= str(new_processing_level_unknown.uid),
    unit=str(new_unit_Q.uid),
    time_aggregation_interval_unit='days',
    intended_time_spacing=most_frequent_diff,
    intended_time_spacing_unit='days'
  )


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

  workspace_observed_properties = hs_api.observedproperties.list(workspace=workspace_WL)

  for observed_property in workspace_observed_properties.items:
    if observed_property.name == 'Water level':
        new_observed_property_WL = observed_property
    elif observed_property.name == 'Streamflow':
        new_observed_property_Q = observed_property

  workspace_units = hs_api.units.list(workspace=workspace_WL)

  for unit in workspace_units.items:
    if unit.name == 'Cubic Meter per Second':
        new_unit_Q = unit
    elif unit.name == 'Meter':
        new_unit_WL_m = unit
    elif unit.name == 'Centimeter':
        new_unit_WL_cm = unit

  workspace_sensors = hs_api.sensors.list(workspace=workspace_WL)

  for sensor in workspace_sensors.items:
    if sensor.name == 'Field Observation':
        new_sensor_obs = sensor

  workspace_processing_levels = hs_api.processinglevels.list(workspace=workspace_WL)

  for processing_level in workspace_processing_levels.items:
    if processing_level.code == '-9999':
        new_processing_level_unknown = processing_level

  isPrivate = not isVisible

  print(folder_WL,  ' - ', datasource_WL,  ' - ', uid, ' - ', code, ' - ', name, ' - ', isVisible, ' - ', isPrivate, ' - ', workspace_WL, ' - ', str(new_observed_property_WL.uid), ' - ', str(new_unit_WL_m.uid), ' - ', str(new_unit_WL_cm.uid), ' - ', str(new_sensor_obs.uid), ' - ', str(new_processing_level_unknown.uid))

  new_observations = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_WL.csv'.format(folder_WL, datasource_WL, code), index_col=0)
  new_observations.index = pd.to_datetime(new_observations.index)
  new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
  new_observations.index = pd.to_datetime(new_observations.index)

  time_diff_days = new_observations.index.to_series().diff() / pd.Timedelta(days=1)
  mode_diff = stats.mode(time_diff_days.dropna())
  most_frequent_diff = mode_diff.mode

  column_water_level = new_observations.columns[0]
  if column_water_level == 'Water Level (cm)':
      unit_wl_id = str(new_unit_WL_cm.uid)
  elif column_water_level == 'Water Level (m)':
      unit_wl_id = str(new_unit_WL_m.uid)
  elif column_water_level == 'Streamflow (m3/s)':
      print('')
      print('')
      print('')
      unit_wl_id = str(new_unit_Q.uid)
  else:
      print('Hay un error en el nombre de la columna de nivel de agua')

  print(f"Uploading Datastream")

  new_datastream = hs_api.datastreams.create(
    name='Observed Water Level',
    description='Observed Water Level Values for {0} - {1}'.format(code, name),
    observation_type='OM_Measurement',
    sampled_medium='Liquid aqueous',
    no_data_value=-9999,
    aggregation_statistic='Continuous',
    time_aggregation_interval=1,
    status='Ongoing',
    result_type='Timeseries',
    value_count=0,
    phenomenon_begin_time=new_observations.index[0],
    phenomenon_end_time=None,
    result_begin_time=new_observations.index[0],
    result_end_time=None,
    is_visible=isVisible,
    is_private=False,
    thing=str(uid),
    sensor=str(new_sensor_obs.uid),
    observed_property=str(new_observed_property_WL.uid),
    processing_level= str(new_processing_level_unknown.uid),
    unit=str(unit_wl_id),
    time_aggregation_interval_unit='days',
    intended_time_spacing=most_frequent_diff,
    intended_time_spacing_unit='days'
  )
