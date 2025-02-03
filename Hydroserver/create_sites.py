import getpass
import pandas as pd
from pprint import pprint
from hydroserverpy import HydroServer

#Uncomment the host of the country you want to use and comment the other ones

#host = 'https://insivumeh-hydroserver.geoglows.org' #Guatemala
#host = 'https://ice-hydroserver.geoglows.org' #Costa Rica
#host = 'https://imhpa-hydroserver.geoglows.org' #Panama
#host = 'https://enee-hydroserver.geoglows.org' #Honduras
#host = 'https://ineter-hydroserver.geoglows.org' #Nicaragua
#host = 'https://marn-hydroserver.geoglows.org' #El Salvador
#host = 'https://mrn-belize-hydroserver.geoglows.org' #Belize
host = 'https://hydroserver.geoglows.org' #Global

username = input("Enter your email: ")
password = getpass.getpass("Enter your password: ")

#Uncomment the host of the country you want to use and comment the other ones

#country = 'Guatemala'
#country = 'Costa Rica'
#country = 'Panama'
#country = 'Honduras'
#country = 'Nicaragua'
#country = 'El Salvador'
#country = 'Belize'

# Initialize HydroServer connection with credentials.
hs_api = HydroServer(
    host=host,
    username=username,
    password=password
)

#stations_df = pd.read_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Panama_new_Stations.csv")
#stations_df = pd.read_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/El_Salvador_new_Stations.csv")
stations_df = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations.csv", keep_default_na=False)

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


