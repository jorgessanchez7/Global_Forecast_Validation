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

#stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Panama_new_Stations.csv', index_col=2)
#stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/El_Salvador_new_Stations.csv', index_col=2)
stations_df = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations.csv", keep_default_na=False, index_col=2)

uids = stations_df.index.to_list()
dataSources = stations_df['Data_Source'].to_list()
codes = stations_df['samplingFeatureCode'].to_list()
names = stations_df['name'].to_list()
folders = stations_df['Folder'].to_list()
Qs = stations_df['Q'].to_list()
WLs = stations_df['WL'].to_list()

for uid, dataSource, code, name, folder, q, wl in zip(uids, dataSources, codes, names, folders, Qs, WLs):

  try:

    thing = hs_api.things.get(uid=uid)
    thing_datastreams = thing.datastreams

    if (len(thing_datastreams)==0):
        print(uid, ' - ', code, ' - ', name, ' - ', q, ' - ', wl)
    elif ((q=='YES' and wl =='YES') and len(thing_datastreams)==1):
        print(uid, ' - ', code, ' - ', name, ' - ', q, ' - ', wl)

  except Exception as e:

    print(e.__str__())
