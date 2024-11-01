import json
import requests
import numpy as np
import pandas as pd
from datetime import datetime

stations_bangladesh = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Observed_Data/South_Asia/Bangladesh/Selected_Stations_Bangladesh_WL_v0.csv')
stations_bangladesh.dropna(inplace=True)

IDs = stations_bangladesh['Code'].tolist()
COMIDs = stations_bangladesh['COMID'].tolist()
Names = stations_bangladesh['Station'].tolist()
WebIDs = stations_bangladesh['Web_ID'].tolist()

# Create a date range starting from '2010-01-01' to '2024-09-30'
start_date = '2010-01-01'
end_date = '2024-09-30'

# Generate an array with the first and last day of each month
dates = pd.date_range(start=start_date, end=end_date, freq='ME').to_list()
dates = [pd.Timestamp(start_date)] + dates  # Adding the first day of the range

# Ensure the last day '2024-09-30' is included
if dates[-1].strftime('%Y-%m-%d') != end_date:
    dates.append(pd.Timestamp(end_date))

dates_list = [str(date.date()) for date in dates]  # Converting to string format

for id, name, comid, webid in zip(IDs, Names, COMIDs, WebIDs):

    webid = int(webid)

    print(id, ' - ', name, ' - ', comid, ' - ', webid)

    # Initialize lists for storing dates and water levels
    fechas = []
    water_levels = []

    for fecha in dates_list:

        url = 'https://ffwc-api.bdservers.site/data_load/observed-waterlevel-by-station-and-date/{0}/{1}?format=json'.format(webid, fecha)
        #print(url)

        response = requests.get(url)
        data = response.json()  # Assuming the response is a valid JSON

        for entry in data:
            # Parse the wl_date and format as 'YYYY-MM-DD hh:mm:ss'
            date = datetime.strptime(entry['wl_date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            fechas.append(date)

            # Convert waterlevel to an integer
            water_level = float(entry['waterlevel'])
            water_levels.append(water_level)

    df = pd.DataFrame({"Datetime": pd.to_datetime(fechas),"Water Level (m)": water_levels})

    # Set the "Datetime" column as the index
    df.set_index("Datetime", inplace=True)
    df = df.loc[~df.index.duplicated(keep='first')]

    observed_df = df.groupby(df.index.strftime("%Y/%m/%d")).mean()
    observed_df.index = pd.to_datetime(observed_df.index)

    datesObservedDischarge = pd.date_range(observed_df.index[0], observed_df.index[len(observed_df.index) - 1], freq='D')
    df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Water Level (m)'])
    df2.index.name = 'Datetime'

    df3 = df2.fillna(observed_df)

    df3.to_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Observed_Data/South_Asia/Bangladesh/Observed_Data_WL_web/{}_web.csv'.format(id))
    
    