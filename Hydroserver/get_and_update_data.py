import getpass
import datetime
import pandas as pd
from pprint import pprint
from hydroserverpy import HydroServer
from hydroserverpy import HydroServerQualityControl

#Uncomment the host of the country you want to use and comment the other ones

#host = 'https://insivumeh-hydroserver.geoglows.org' #Guatemala
#host = 'https://ice-hydroserver.geoglows.org' #Costa Rica
host = 'https://imhpa-hydroserver.geoglows.org' #Panama
#host = 'https://enee-hydroserver.geoglows.org' #Honduras
#host = 'https://ineter-hydroserver.geoglows.org' #Nicaragua
#host = 'https://marn-hydroserver.geoglows.org' #El Salvador
#host = 'https://mrn-belize-hydroserver.geoglows.org' #Belize

username = input("Enter your email: ")
password = getpass.getpass("Enter your password: ")

#Uncomment the host of the country you want to use and comment the other ones

#country = 'Guatemala'
#country = 'Costa Rica'
country = 'Panama'
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

stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Panama_update_Station.csv')
stations_df = stations_df.loc[stations_df['country_name'] == country]

# Get all Datastreams
datastreams = hs_api.datastreams.list()
# Get primary owned Datastreams
primary_owned_datastreams = hs_api.datastreams.list(primary_owned_only=True)

# Get owned Datastreams
owned_datastreams = hs_api.datastreams.list(owned_only=True)
pprint(owned_datastreams)

# Get primary owned Things
primary_owned_things = hs_api.things.list(primary_owned_only=True)
pprint(primary_owned_things)

# Convert list of dictionaries to DataFrame and set 'uid' as the index
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

df = pd.DataFrame(thing_data)

merged_df = pd.merge(stations_df, df[['sampling_feature_code', 'uid']], left_on='samplingFeatureCode', right_on='sampling_feature_code', how='left')

merged_df.set_index('uid', inplace=True)

print(merged_df)

uids = merged_df.index.to_list()
dataSources = merged_df['Data_Source'].to_list()
codes = merged_df['samplingFeatureCode'].to_list()
names = merged_df['name'].to_list()

for uid, dataSource, code, name in zip(uids, dataSources, codes, names):

    print(uid, ' - ', code, ' - ', name)

    try:

        thing = hs_api.things.get(uid=uid)
        thing_datastreams = thing.datastreams

        for single_datastream in thing_datastreams:
            if single_datastream.name == 'Observed Streamflow':

                datastream = hs_api.datastreams.get(uid=str(single_datastream.uid))

                # Get Observations of a Datastream between two timestamps
                observations_df = datastream.get_observations(
                    include_quality=True,
                    start_time=datetime.datetime(year=1900, month=1, day=1),
                    end_time=datetime.datetime(year=2025, month=12, day=31)
                )

                hs_quality_control = HydroServerQualityControl(
                    datastream_id=datastream.uid,
                    observations=observations_df
                )

                new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Observed_Data/{0}/{1}_Q.csv'.format(dataSource, code))

                column_discharge = new_observations.columns[1]

                new_observations = new_observations.rename(columns={"Datetime": "timestamp", column_discharge: "value"})
                new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

                # Convert 'timestamp' columns to datetime objects if they're not already
                observations_df['timestamp'] = pd.to_datetime(observations_df['timestamp'])
                new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp'])

                # Perform an inner join on 'timestamp' to find common rows
                common_rows = pd.merge(observations_df[['timestamp']], new_observations[['timestamp']], on='timestamp', how='inner')

                # Drop the common rows from df2
                new_observations = new_observations[~new_observations['timestamp'].isin(common_rows['timestamp'])]

                print(new_observations)

                new_points = []
                for index, row in new_observations.iterrows():
                    new_points.append([pd.Timestamp(row['timestamp']), row['value']])

                if len(new_points) > 0:
                    # Add the new values points
                    hs_quality_control.add_points(points=new_points)

                # Step 1: Create a dictionary of original values for the removed indices
                original_values = {idx: observations_df.loc[idx, 'value'] for idx in gap_indices}

                # Step 2: Get the indices of the added points in the modified DataFrame
                added_point_indices = hs_quality_control.observations[hs_quality_control.observations['timestamp'].isin([observations_qc_df.loc[idx, 'timestamp'] for idx in gap_indices])].index.tolist()

                # Step 3: Use the `change_values` function to restore the original values
                for idx, original_value in zip(added_point_indices, original_values.values()):
                    hs_quality_control.change_values(index_list=[idx], operator="ASSIGN", value=original_value)

            elif single_datastream.name == 'Observed Water Level':

                datastream = hs_api.datastreams.get(uid=str(single_datastream.uid))

                # Get Observations of a Datastream between two timestamps
                observations_df = datastream.get_observations(
                    include_quality=True,
                    start_time=datetime.datetime(year=1900, month=1, day=1),
                    end_time=datetime.datetime(year=2025, month=12, day=31)
                )

                hs_quality_control = HydroServerQualityControl(
                    datastream_id=datastream.uid,
                    observations=observations_df
                )

                new_observations = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/{0}/{1}_WL.csv'.format(dataSource, code))

                column_water_level = new_observations.columns[1]

                new_observations = new_observations.rename(
                    columns={"Datetime": "timestamp", column_water_level: "value"})
                new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp']).dt.tz_localize('UTC')

                # Convert 'timestamp' columns to datetime objects if they're not already
                observations_df['timestamp'] = pd.to_datetime(observations_df['timestamp'])
                new_observations['timestamp'] = pd.to_datetime(new_observations['timestamp'])

                # Perform an inner join on 'timestamp' to find common rows
                common_rows = pd.merge(observations_df[['timestamp']], new_observations[['timestamp']], on='timestamp', how='inner')

                # Drop the common rows from df2
                new_observations = new_observations[~new_observations['timestamp'].isin(common_rows['timestamp'])]

                new_points = []
                for index, row in new_observations.iterrows():
                    new_points.append([pd.Timestamp(row['timestamp']), row['value']])

                if len(new_points) > 0:
                    # Add the new values points
                    hs_quality_control.add_points(points=new_points)

                    # Step 1: Create a dictionary of original values for the removed indices
                    original_values = {idx: observations_df.loc[idx, 'value'] for idx in gap_indices}

                    # Step 2: Get the indices of the added points in the modified DataFrame
                    added_point_indices = hs_quality_control.observations[hs_quality_control.observations['timestamp'].isin([observations_qc_df.loc[idx, 'timestamp'] for idx in gap_indices])].index.tolist()

                    # Step 3: Use the `change_values` function to restore the original values
                    for idx, original_value in zip(added_point_indices, original_values.values()):
                        hs_quality_control.change_values(index_list=[idx], operator="ASSIGN", value=original_value)
                
    except Exception as e:
        print(e)
