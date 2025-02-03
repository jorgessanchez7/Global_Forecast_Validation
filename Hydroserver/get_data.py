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

stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Central_America/Panama_Selected_Stations.csv', index_col=2)
stations_df = stations_df.loc[stations_df['country_name'] == country]

# Get all Datastreams
datastreams = hs_api.datastreams.list()
# Get primary owned Datastreams
#primary_owned_datastreams = hs_api.datastreams.list(primary_owned_only=True)

# Get owned Datastreams
owned_datastreams = hs_api.datastreams.list(owned_only=True)
#pprint(owned_datastreams)

print(stations_df)

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

                units = str(single_datastream.unit.symbol)

                datastream = hs_api.datastreams.get(uid=str(single_datastream.uid))

                # Get Observations of a Datastream between two timestamps
                observations_df = datastream.get_observations(
                    include_quality=True,
                    start_time=datetime.datetime(year=1900, month=1, day=1),
                    end_time=datetime.datetime(year=2025, month=12, day=31)
                )

                observed_values = observations_df.copy()
                observed_values.drop(['quality_code', 'result_quality'], axis=1, inplace=True)
                observed_values.rename(columns={'timestamp': 'Datetime', 'value': '{0} ({1})'.format(single_datastream.name, units)}, inplace=True)
                observed_values.set_index('Datetime', inplace=True)
                observed_values.index = pd.to_datetime(observed_values.index)
                observed_values.index = observed_values.index.to_series().dt.strftime("%Y-%m-%d")
                observed_values.index = pd.to_datetime(observed_values.index)

                observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, dataSource, code), index_col=0)
                observations.rename(columns={'Streamflow (m3/s)': '{0} ({1})'.format(single_datastream.name, units)}, inplace=True)
                observations.index = pd.to_datetime(observations.index)
                observations.index = observations.index.to_series().dt.strftime("%Y-%m-%d")
                observations.index = pd.to_datetime(observations.index)

                # Compare rounded dataframes
                print(observed_values.equals(observations))

            elif single_datastream.name == 'Observed Water Level':

                units = str(single_datastream.unit.symbol)

                datastream = hs_api.datastreams.get(uid=str(single_datastream.uid))

                # Get Observations of a Datastream between two timestamps
                observations_df = datastream.get_observations(
                    include_quality=True,
                    start_time=datetime.datetime(year=1900, month=1, day=1),
                    end_time=datetime.datetime(year=2025, month=12, day=31)
                )

                observed_values = observations_df.copy()
                observed_values.drop(['quality_code', 'result_quality'], axis=1, inplace=True)
                observed_values.rename(columns={'timestamp': 'Datetime', 'value': '{0} ({1})'.format(single_datastream.name, units)}, inplace=True)
                observed_values.set_index('Datetime', inplace=True)
                observed_values.index = pd.to_datetime(observed_values.index)
                observed_values.index = observed_values.index.to_series().dt.strftime("%Y-%m-%d")
                observed_values.index = pd.to_datetime(observed_values.index)

                observations = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_WL.csv'.format(folder, dataSource, code), index_col=0)
                observations.rename(columns={'Water Level ({})'.format(units): '{0} ({1})'.format(single_datastream.name, units)}, inplace=True)
                observations.index = pd.to_datetime(observations.index)
                observations.index = observations.index.to_series().dt.strftime("%Y-%m-%d")
                observations.index = pd.to_datetime(observations.index)

                # Compare rounded dataframes
                print(observed_values.equals(observations))

    except Exception as e:
        print(e)
