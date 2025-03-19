import folium
import getpass
import pandas as pd
from pprint import pprint
from hydroserverpy import HydroServer
from folium.plugins import MarkerCluster

host = 'https://enee-hydroserver.geoglows.org' #Honduras

username = input("Enter your email: ")
password = input("Enter your password: ")

# Initialize HydroServer connection with credentials.
hs_api = HydroServer(
    host=host,
    username=username,
    password=password
)

country = 'Honduras'

stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras_new_Stations.csv', index_col=2)

owned_datastreams = hs_api.datastreams.list(owned_only=True)
pprint(owned_datastreams)

'''Adding Observations'''

uids = stations_df.index.to_list()
dataSources = stations_df['Data_Source'].to_list()
codes = stations_df['samplingFeatureCode'].to_list()
names = stations_df['name'].to_list()
folders = stations_df['Folder'].to_list()

for uid, dataSource, code, name, folder in zip(uids, dataSources, codes, names, folders):

  print(uid, ' - ', code, ' - ', name)

  thing = hs_api.things.get(uid=uid)
  thing_datastreams = thing.datastreams

  for single_datastream in thing_datastreams:

    print(single_datastream.uid, ' - ', single_datastream.name)

    try:

      if single_datastream.name == 'Observed Streamflow':
        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/Data/{0}_Q.csv'.format(code))

        column_discharge = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_discharge: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

      elif single_datastream.name == 'Observed Streamflow (Monthly)':
        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/Data/{0}_Q.csv'.format(code))

        column_discharge = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_discharge: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

      elif single_datastream.name == 'Observed Streamflow (15 Minutes) Quality Controlled':
        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/Data/{0}_Q_RT_qq.csv'.format(code))

        column_discharge = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_discharge: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

      elif single_datastream.name == 'Observed Streamflow (15 Minutes)':
        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/Data/{0}_Q_RT.csv'.format(code))

        column_discharge = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_discharge: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

      elif single_datastream.name == 'Observed Water Level':
        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/Data/{0}_WL.csv'.format(code))

        column_water_level = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_water_level: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

      elif single_datastream.name == 'Observed Water Level (15 Minutes)':

        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras/Datos_Temporales/Data/{0}_WL_RT.csv'.format(code))

        column_water_level = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_water_level: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

    except Exception as e:

      print(e.__str__())

print('Las series de tiempo se han subido!. Revise la salida de este código para ver posibles errores')
