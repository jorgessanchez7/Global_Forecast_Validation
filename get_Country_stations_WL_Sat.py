import math
import numpy as np
import pandas as pd
from geopy.geocoders import Nominatim


# initialize Nominatim API
geolocator = Nominatim(user_agent="geoapiExercises")

total_stations_Q = pd.read_csv('/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL_Sat.csv')

COMIDs = total_stations_Q['COMID'].tolist()
Names = total_stations_Q['Station'].tolist()
latitudes = total_stations_Q['Latitude'].tolist()
longitudes = total_stations_Q['Longitude'].tolist()
paises = total_stations_Q['Country'].tolist()

table = []

for comid, name, latitude, longitude, pais in zip(COMIDs, Names, latitudes, longitudes, paises):

    print(comid, ' - ', name)

    Latitude = str(latitude)
    Longitude = str(longitude)

    location = geolocator.reverse(Latitude + "," + Longitude, language='en')

    address = location.raw['address']

    if math.isnan(pais):
        country = address.get('country', '')
    else:
        country = pais
    print(country)

    table.append([comid, name, latitude, longitude, country])

table_df = pd.DataFrame(table, columns=['COMID', 'Station', 'Latitude', 'Longitude', 'Country'])

table_df.to_csv('/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL_Sat_v2.csv')