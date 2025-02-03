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

# Get all Things
things = hs_api.things.list()

# Get owned Things
owned_things = hs_api.things.list(owned_only=True)

# Get primary owned Things
primary_owned_things = hs_api.things.list(primary_owned_only=True)
pprint(primary_owned_things)

thing_data = []
for thing in primary_owned_things:
    # Extract only the desired attributes and convert them to native Python types
    thing_dict = {
        'uid': str(thing.uid),
        'sampling_feature_code': str(thing.sampling_feature_code),
        'sampling_feature_type': int(thing.sampling_feature_type),
        'name': str(thing.name),
        'latitude': float(thing.latitude),
        'longitude': float(thing.longitude),
        'elevation_m': float(thing.elevation_m),
        'elevation_datum': str(thing.elevation_datum),
        'state': str(thing.state),
        'county': str(thing.county),
        'country': str(thing.country),
        'site_type': str(thing.site_type),
        'description': str(thing.description),
        'data_disclaimer': str(thing.data_disclaimer),
    }
    thing_data.append(thing_dict)

# Convert list of dictionaries to DataFrame and set 'uid' as the index
df = pd.DataFrame(thing_data).set_index('uid')

#df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Panama_Stations_Hydroserver.csv")
#df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/El_Salvador_Stations_Hydroserver.csv")
df.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Sites_Hydroserver.csv")