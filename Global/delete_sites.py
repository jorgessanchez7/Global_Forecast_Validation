import getpass
import requests
import pandas as pd
from hydroserverpy import HydroServer
from requests.auth import HTTPBasicAuth

host = 'https://hydroserver.geoglows.org' #Global

username = input("Enter your email: ")
password = getpass.getpass("Enter your password: ")

# Initialize HydroServer connection with credentials.
hs_api = HydroServer(
    host=host,
    username=username,
    password=password
)

headers = {
    "accept": "*/*"
}

stations_df = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations_delete_left.csv", keep_default_na=False, index_col=2)

uids = stations_df.index.to_list()
dataSources = stations_df['Data_Source'].to_list()
codes = stations_df['samplingFeatureCode'].to_list()
names = stations_df['name'].to_list()
visibility = stations_df['isVisible'].to_list()
folders = stations_df['Folder'].to_list()

for uid, dataSource, code, name, folder, isVisible in zip(uids, dataSources, codes, names, folders, visibility):
    
    print(uid, ' - ', code, ' - ', name)

    url = "https://hydroserver.geoglows.org/api/data/things/{0}".format(uid)

    response = requests.delete(url, headers=headers, auth=HTTPBasicAuth(username, password))

    print("Status Code:", response.status_code)
    #try:
    #    print("Response:", response.json())
    #except Exception:
    #    print("Response could not be parsed as JSON.")


