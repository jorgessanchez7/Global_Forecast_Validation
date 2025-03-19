import getpass
import pandas as pd
from pprint import pprint
from hydroserverpy import HydroServer

#Uncomment the host of the country you want to use and comment the other ones

host = 'https://enee-hydroserver.geoglows.org' #Honduras

username = input("Enter your email: ")
password = getpass.getpass("Enter your password: ")

#Uncomment the host of the country you want to use and comment the other ones

country = 'Honduras'

# Initialize HydroServer connection with credentials.
hs_api = HydroServer(
    host=host,
    username=username,
    password=password
)

stations_df = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Honduras_new_Stations.csv")

print(stations_df)

# Iterate over the sampled stations and upload them
uploaded_stations = []
for i, row in stations_df.iterrows():
    print(f"Uploading station {i+1}/{len(stations_df)}: {row['name']}")
    new_thing = hs_api.things.create(
        name=row['name'],
        description=row['description'],
        sampling_feature_type=str(row['samplingFeatureType']),
        sampling_feature_code=str(row['samplingFeatureCode']),
        site_type=row['siteType'],
        latitude=str(row['latitude']),
        longitude=str(row['longitude']),
        elevation_m=str(row['elevation_m']),
        elevation_datum=str(row['elevationDatum']),
        state=row['state'],
        county=row['county'],
        country=row['country'],
        data_disclaimer=row['dataDisclaimer']
    )
    uploaded_stations.append(new_thing)

# Get all Things
things = hs_api.things.list()

# Get owned Things
owned_things = hs_api.things.list(owned_only=True)

# Get primary owned Things
primary_owned_things = hs_api.things.list(primary_owned_only=True)
pprint(primary_owned_things)

# Get Thing with a given ID
# let's get the first station that we own
station_uid = primary_owned_things[0].uid
thing = hs_api.things.get(uid=station_uid)
pprint(thing)


