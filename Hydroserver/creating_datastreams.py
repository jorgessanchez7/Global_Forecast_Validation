import folium
import getpass
import pandas as pd
from pprint import pprint
from hydroserverpy import HydroServer
from folium.plugins import MarkerCluster

#host = 'https://insivumeh-hydroserver.geoglows.org' #Guatemala
#host = 'https://ice-hydroserver.geoglows.org' #Costa Rica
#host = 'https://imhpa-hydroserver.geoglows.org' #Panama
#host = 'https://enee-hydroserver.geoglows.org' #Honduras
#host = 'https://ineter-hydroserver.geoglows.org' #Nicaragua
#host = 'https://marn-hydroserver.geoglows.org' #El Salvador
#host = 'https://mrn-belize-hydroserver.geoglows.org' #Belize
host = 'https://hydroserver.geoglows.org' #Global

username = input("Enter your email: ")
password = input("Enter your password: ")

# Initialize HydroServer connection with credentials.
hs_api = HydroServer(
    host=host,
    username=username,
    password=password
)

#country = 'Guatemala'
#country = 'Costa Rica'
#country = 'Panama'
#country = 'Honduras'
#country = 'Nicaragua'
#country = 'El Salvador'
#country = 'Belize'
country = ''

'''DataStreams'''
# Get all Observed Properties
observed_properties = hs_api.observedproperties.list()
# Get all Observed Properties excluding templates
owned_and_unowned_observed_properties = hs_api.observedproperties.list(include_templates=False)
# Get owned Observed Properties including templates
owned_and_template_observed_properties = hs_api.observedproperties.list(include_unowned=False)
# Get Observed Property templates
template_observed_properties = hs_api.observedproperties.list(include_owned=False, include_templates=True)

# Get owned Observed Properties
owned_observed_properties = hs_api.observedproperties.list(include_templates=False, include_unowned=False)
pprint(owned_observed_properties)

if len(owned_observed_properties) > 0:
  for observed_property in owned_observed_properties:
    if observed_property.name == 'Water level':
      new_observed_property_WL = observed_property
    elif observed_property.name == 'Streamflow':
      new_observed_property_Q = observed_property
else:
  # Create Observed Properties in HydroServer
  new_observed_property_Q = hs_api.observedproperties.create(
      name='Streamflow',
      definition='The volume of water flowing past a fixed point.  Equivalent to discharge',
      description='Originally from the CUAHSI HIS VariableNameCV.  See: http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=VariableNameCV.',
      type='Hydrology', # further reasearch
      code='Q',
  )

  new_observed_property_WL = hs_api.observedproperties.create(
      name='Water level',
      definition='Water level relative to datum. The datum may be local or global such as NGVD 1929 and should be specified in the method description for associated data values.',
      description='Originally from the CUAHSI HIS VariableNameCV.  See: http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=VariableNameCV.',
      type='Hydrology', # further reasearch
      code='WL',
  )

'''Units'''
# Get all Units
units = hs_api.units.list()
# Get all Units excluding templates
owned_and_unowned_units = hs_api.units.list(include_templates=False)
# Get owned Units including templates
owned_and_template_units = hs_api.units.list(include_unowned=False)
# Get Units templates
template_units = hs_api.units.list(include_owned=False, include_templates=True)

# Get owned Units
owned_units = hs_api.units.list(include_templates=False, include_unowned=False)
pprint(owned_units)

if len(owned_units) > 0:
  for unit in owned_units:
      if unit.name == 'Cubic Meter per Second':
          new_unit_Q = unit
      elif unit.name == 'Meter':
          new_unit_WL_m = unit
      elif unit.name == 'Centimeter':
          new_unit_WL_cm = unit
else:
  # Create a new Unit on HydroServer
  new_unit_Q = hs_api.units.create(
      name='Cubic Meter per Second',
      symbol='m^3/s',
      definition='http://qudt.org/vocab/unit#CubicMeterPerSecond',
      type='Volumetric flow rate'
  )

  new_unit_WL_m = hs_api.units.create(
      name='Meter',
      symbol='m',
      definition='http://qudt.org/vocab/unit#Meter',
      type='Length'
  )

  new_unit_WL_cm = hs_api.units.create(
      name='Centimeter',
      symbol='cm',
      definition='http://qudt.org/vocab/unit#Centimeter',
      type='Length'
  )


'''Sensors'''
# Get all Sensors
sensors = hs_api.sensors.list()
# Get all Sensors excluding templates
owned_and_unowned_sensors = hs_api.sensors.list(include_templates=False)
# Get owned Sensors including templates
owned_and_template_sensors = hs_api.sensors.list(include_unowned=False)
# Get Sensors templates
template_sensors = hs_api.sensors.list(include_owned=False, include_templates=True)

# Get owned Sensors
owned_sensors = hs_api.sensors.list(include_templates=False, include_unowned=False)
pprint(owned_sensors)

if len(owned_sensors) > 0:
  for sensor in owned_sensors:
    if sensor.name == 'Field Observation':
      new_sensor_obs = sensor
else:
  # Create a new Sensor on HydroServer
  new_sensor_obs = hs_api.sensors.create(
      name='Field Observation',
      description='Data reflect the measurement the sensor is designed to make, reported at the frequency of data collection',
      encoding_type='application/json',
      manufacturer='',
      model='',
      model_link='',
      method_type='Observation',
      method_link='http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=ValueTypeCV&id=1141579105',
      method_code='obs'
  )

'''Processing Levels'''
# Get all Processing Levels
processing_levels = hs_api.processinglevels.list()
# Get all Processing Levels excluding templates
owned_and_unowned_processing_levels = hs_api.processinglevels.list(include_templates=False)
# Get owned Processing Levels including templates
owned_and_template_processing_levels = hs_api.processinglevels.list(include_unowned=False)
# Get Processing Levels templates
template_processing_levels = hs_api.processinglevels.list(include_owned=False, include_templates=True)

# Get owned Processing Levels
owned_processing_levels = hs_api.processinglevels.list(include_templates=False, include_unowned=False)
pprint(owned_processing_levels)

if len(owned_processing_levels) > 0:
    for processing_level in owned_processing_levels:
        if processing_level.code == '-9999':
            new_processing_level_unknown = processing_level
else:
    # Create a new Processing Level on HydroServer
    new_processing_level_unknown = hs_api.processinglevels.create(
        code='-9999',
        definition='Unknown',
        explanation='The quality control level is unknown'
    )

#################
'''DataStreams'''
#################

# Get all Datastreams
#datastreams = hs_api.datastreams.list()
# Get primary owned Datastreams
#primary_owned_datastreams = hs_api.datastreams.list(primary_owned_only=True)

# Get owned Datastreams
#owned_datastreams = hs_api.datastreams.list(owned_only=True)
#pprint(owned_datastreams)

#stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Panama_new_Stations.csv', index_col=2)
#stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/El_Salvador_new_Stations.csv', index_col=2)
stations_df = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations_missing.csv", keep_default_na=False, index_col=2)

'''Discharge'''
discharge_stations = stations_df.loc[stations_df['Q'] == 'YES']

uids_Q = discharge_stations.index.to_list()
dataSources_Q = discharge_stations['Data_Source'].to_list()
codes_Q = discharge_stations['samplingFeatureCode'].to_list()
names_Q = discharge_stations['name'].to_list()
visibility_Q = discharge_stations['isVisible'].to_list()
folders_Q = discharge_stations['Folder'].to_list()

for uid, dataSource, code, name, folder, isVisible in zip(uids_Q, dataSources_Q, codes_Q, names_Q, folders_Q, visibility_Q):

  try:

    print(uid, ' - ', code, ' - ', name)

    new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, dataSource, code), index_col=0)
    new_observations.index = pd.to_datetime(new_observations.index)
    new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
    new_observations.index = pd.to_datetime(new_observations.index)

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
      is_visible=True,
      is_data_visible=isVisible,
      thing_id=str(uid),
      sensor_id=str(new_sensor_obs.uid),
      observed_property_id=str(new_observed_property_Q.uid),
      processing_level_id= str(new_processing_level_unknown.uid),
      unit_id=str(new_unit_Q.uid),
      time_aggregation_interval_units='days',
      intended_time_spacing=1,
      intended_time_spacing_units='days'
    )

  except Exception as e:

    print(e.__str__())

'''Water Level'''
waterlevel_stations = stations_df.loc[stations_df['WL'] == 'YES']

uids_WL = waterlevel_stations.index.to_list()
dataSources_WL = waterlevel_stations['Data_Source'].to_list()
codes_WL = waterlevel_stations['samplingFeatureCode'].to_list()
names_WL = waterlevel_stations['name'].to_list()
visibility_WL = waterlevel_stations['isVisible'].to_list()
folders_WL = waterlevel_stations['Folder'].to_list()

for uid, dataSource, code, name, folder, isVisible in zip(uids_WL, dataSources_WL, codes_WL, names_WL, folders_WL, visibility_WL):

  try:

    print(uid, ' - ', code, ' - ', name)

    new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_WL.csv'.format(folder, dataSource, code), index_col=0)
    new_observations.index = pd.to_datetime(new_observations.index)
    new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
    new_observations.index = pd.to_datetime(new_observations.index)

    column_water_level = new_observations.columns[0]
    if column_water_level == 'Water Level (cm)':
        unit_wl_id = str(new_unit_WL_cm.uid)
    elif column_water_level == 'Water Level (m)':
        unit_wl_id = str(new_unit_WL_m.uid)
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
      is_visible=True,
      is_data_visible=isVisible,
      thing_id=str(uid),
      sensor_id=str(new_sensor_obs.uid),
      observed_property_id=str(new_observed_property_WL.uid),
      processing_level_id= str(new_processing_level_unknown.uid),
      unit_id=str(unit_wl_id),
      time_aggregation_interval_units='days',
      intended_time_spacing=1,
      intended_time_spacing_units='days'
    )

    if country == 'El Salvador':

        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_WL_abs.csv'.format(folder, dataSource, code), index_col=0)
        new_observations.index = pd.to_datetime(new_observations.index)
        new_observations.index = new_observations.index.to_series().dt.strftime("%Y-%m-%d")
        new_observations.index = pd.to_datetime(new_observations.index)

        column_water_level = new_observations.columns[0]
        if column_water_level == 'Water Level (cm)':
            unit_wl_id = str(new_unit_WL_cm.uid)
        elif column_water_level == 'Water Level (m)':
            unit_wl_id = str(new_unit_WL_m.uid)
        else:
            print('Hay un error en el nombre de la columna de nivel de agua')

        print(f"Uploading Datastream")

        new_datastream = hs_api.datastreams.create(
            name='Observed Absolute Water Level',
            description='Observed Absolute Water Level Values for {0} - {1}'.format(code, name),
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
            is_visible=True,
            is_data_visible=isVisible,
            thing_id=str(uid),
            sensor_id=str(new_sensor_obs.uid),
            observed_property_id=str(new_observed_property_WL.uid),
            processing_level_id=str(new_processing_level_unknown.uid),
            unit_id=str(unit_wl_id),
            time_aggregation_interval_units='days',
            intended_time_spacing=1,
            intended_time_spacing_units='days'
        )

  except Exception as e:

    print(e.__str__())

owned_datastreams = hs_api.datastreams.list(owned_only=True)
pprint(owned_datastreams)
