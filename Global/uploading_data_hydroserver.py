import folium
import getpass
import pandas as pd
from pprint import pprint
from hydroserverpy import HydroServer
from folium.plugins import MarkerCluster

host = 'https://hydroserver.geoglows.org'

username = input("Enter your email: ")
password = input("Enter your password: ")

# Initialize HydroServer connection with credentials.
hs_api = HydroServer(
    host=host,
    username=username,
    password=password
)

stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations.csv', index_col=0)

owned_datastreams = hs_api.datastreams.list(owned_only=True)
#pprint(owned_datastreams)

'''Adding Observations'''

uids = stations_df.index.to_list()
dataSources = stations_df['Data_Source'].to_list()
codes = stations_df['samplingFeatureCode'].to_list()
names = stations_df['name'].to_list()
folders = stations_df['Folder'].to_list()

for uid, dataSource, code, name, folder in zip(uids, dataSources, codes, names, folders):

  print(uid, ' - ', code, ' - ', name)

  try:

    thing = hs_api.things.get(uid=uid)
    thing_datastreams = thing.datastreams

    for single_datastream in thing_datastreams:
      if single_datastream.name == 'Observed Streamflow':
        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, dataSource, code))

        column_discharge = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_discharge: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

      elif single_datastream.name == 'Observed Water Level':
        new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_WL.csv'.format(folder, dataSource, code))

        column_water_level = new_observations.columns[1]

        new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_water_level: "value"})
        new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

        print(f"Uploading Data to Datastream")
        single_datastream.load_observations(new_observations)

  except Exception as e:

    print(e.__str__())

print('Todas las series de tiempo se han subido!')
