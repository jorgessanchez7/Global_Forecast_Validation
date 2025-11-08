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


for country_id in countries_id:

  new_observed_property_Q = hs_api.observedproperties.create(
      name='Streamflow',
      definition='The volume of water flowing past a fixed point.  Equivalent to discharge',
      description='Originally from the CUAHSI HIS VariableNameCV.  See: http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=VariableNameCV.',
      observed_property_type='Hydrology',
      code='Q',
      workspace=country_id
  )

  new_observed_property_WL = hs_api.observedproperties.create(
      name='Water level',
      definition='Water level relative to datum. The datum may be local or global such as NGVD 1929 and should be specified in the method description for associated data values.',
      description='Originally from the CUAHSI HIS VariableNameCV.  See: http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=VariableNameCV.',
      observed_property_type='Hydrology',
      code='WL',
      workspace=country_id
  )


## Units

units = hs_api.units.list()

print(units)


for country_id in countries_id:

  workspace_units = hs_api.units.list(workspace=country_id)

  print(workspace_units)


for country_id in countries_id:

  new_unit_Q = hs_api.units.create(
      name='Cubic Meter per Second',
      symbol='m^3/s',
      definition='http://qudt.org/vocab/unit#CubicMeterPerSecond',
      unit_type='Volumetric flow rate',
      workspace=country_id
  )

  new_unit_WL_m = hs_api.units.create(
      name='Meter',
      symbol='m',
      definition='http://qudt.org/vocab/unit#Meter',
      unit_type='Length',
      workspace=country_id
  )

  new_unit_WL_cm = hs_api.units.create(
      name='Centimeter',
      symbol='cm',
      definition='http://qudt.org/vocab/unit#Centimeter',
      unit_type='Length',
      workspace=country_id
  )


## Sensors

sensors = hs_api.sensors.list()

print(sensors)


for country_id in countries_id:

  new_sensor_obs = hs_api.sensors.create(
      name='Field Observation',
      description='Data reflect the measurement the sensor is designed to make, reported at the frequency of data collection',
      encoding_type='application/json',
      manufacturer='',
      sensor_model='',
      sensor_model_link='',
      method_type='Observation',
      method_link='http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=ValueTypeCV&id=1141579105',
      method_code='obs',
      workspace=country_id
  )


## Processing Levels

processing_levels = hs_api.processinglevels.list()

print(processing_levels)


for country_id in countries_id:

  workspace_processing_levels = hs_api.processinglevels.list(workspace=country_id)

  print(workspace_processing_levels)


for country_id in countries_id:

  new_processing_level_unknown = hs_api.processinglevels.create(
      code='-9999',
      definition='Unknown',
      explanation='The quality control level is unknown',
      workspace=country_id
  )

  new_processing_level_0 = hs_api.processinglevels.create(
    code='0',
    definition='Raw',
    explanation='Data have not been processed or quality controlled.',
    workspace=country_id
  )

  new_processing_level_1 = hs_api.processinglevels.create(
      code='1',
      definition='Quality controlled data',
      explanation='Quality controlled data that have passed quality assurance procedures such as routine estimation of timing and sensor calibration or visual inspection and removal of obvious errors. An example is USGS published streamflow records following parsing through USGS quality control procedures.',
      workspace=country_id
  )

  new_processing_level_2 = hs_api.processinglevels.create(
      code='2',
      definition='Derived products',
      explanation='Derived products that require scientific and technical interpretation and may include multiple-sensor data. An example is basin average precipitation derived from rain gauges using an interpolation procedure.',
      workspace=country_id
  )

  new_processing_level_3 = hs_api.processinglevels.create(
      code='3',
      definition='Interpreted products',
      explanation='Interpreted products that require researcher driven analysis and interpretation, model-based interpretation using other data and/or strong prior assumptions. An example is basin average precipitation derived from the combination of rain gauges and radar return data.',
      workspace=country_id
  )

  new_processing_level_4 = hs_api.processinglevels.create(
      code='4',
      definition='Knowledge products',
      explanation='Knowledge products that require researcher driven scientific interpretation and multidisciplinary data integration and include model-based interpretation using other data and/or strong prior assumptions. An example is percentages of old or new water in a hydrograph inferred from an isotope analysis.',
      workspace=country_id
  )

