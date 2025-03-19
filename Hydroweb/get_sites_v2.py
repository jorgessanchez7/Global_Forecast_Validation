import getpass
import pandas as pd
from pprint import pprint
from hydroserverpy import HydroServer

host = 'https://hydroweb-hydroserver.geoglows.org' #Global

username = input("Enter your email: ")
password = getpass.getpass("Enter your password: ")

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

df.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Hydroweb_Hydroserver_Stations.csv")