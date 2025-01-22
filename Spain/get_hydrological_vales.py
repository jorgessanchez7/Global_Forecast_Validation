import pandas as pd

total_stations = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Spain/Station_Network/Total_Stations_Spain.csv")

organizations = total_stations['Organization'].to_list()
ids = total_stations['ID'].to_list()

def build_time_series(station_id, organization):
    """Build time series dataframes for water level and streamflow.

    Args:
        file_path (str): Path to the input CSV file.
        station_id (int): ID of the station.
        organization (str): Name of the organization.

    Returns:
        tuple: Two DataFrames, one for water level and one for streamflow.
    """
    # Read the CSV file
    df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Spain/Historic_Data/row_data/afliq_{0}.csv'.format(organization), sep=';', skipinitialspace=True)

    # Filter the data for the given station ID
    df_station = df[df['indroea'] == station_id].copy()

    # Rename columns
    df_station.rename(columns={
        'fecha': 'Datetime',
        'altura': 'Water Level (m)',
        'caudal': 'Streamflow (m3/s)'
    }, inplace=True)

    # Convert the 'Datetime' column to datetime type
    df_station['Datetime'] = pd.to_datetime(df_station['Datetime'], format='%d/%m/%Y')

    # Sort by date
    df_station.sort_values('Datetime', inplace=True)

    # Set 'Datetime' as the index
    df_station.set_index('Datetime', inplace=True)

    # Ensure time series starts and ends with valid values
    df_station = df_station.dropna(how='all')  # Drop rows where all values are NaN

    # Check if the station has no valid data
    if df_station.empty:
        return None, None, pd.DataFrame({
            'Station ID': [station_id],
            'Organization': [organization],
            'Status': ['No Data']
        })

    # Create separate DataFrames for water level and streamflow
    water_level_df = df_station[['Water Level (m)']].copy()
    streamflow_df = df_station[['Streamflow (m3/s)']].copy()

    # Add missing dates to ensure a continuous time series
    full_range = pd.date_range(start=water_level_df.index.min(), end=water_level_df.index.max(), freq='D')
    water_level_df = water_level_df.reindex(full_range)
    streamflow_df = streamflow_df.reindex(full_range)

    return water_level_df, streamflow_df, None

no_data_station = []

for id, organization in zip(ids, organizations):

    print(id, ' - ', organization)

    water_level_df, streamflow_df, no_data_df = build_time_series(id, organization)

    if no_data_df is None:

        water_level_df.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Spain/Historic_Data/Water_Level/{}.csv".format(id))
        streamflow_df.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Spain/Historic_Data/Discharge/{}.csv".format(id))

    else:

        no_data_station.append(id)

print(no_data_station)
