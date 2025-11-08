import os
import unidecode
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

stations = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\hydroweb_metadata_country.csv")

countries = stations['country_name'].to_list()
countries = list(set(countries))
countries = sorted(countries)

station_data = []

for country in countries:

    stations_country = stations[stations['country_name'] == country]

    country = country.replace(" ", "_")

    IDs = stations_country.index.to_list()
    Codes = stations_country['samplingFeatureCode'].to_list()
    Names = stations_country['name'].to_list()
    Latitudes = stations_country['latitude'].to_list()
    Longitudes = stations_country['longitude'].to_list()
    Elevations = stations_country['elevation_m'].to_list()
    Institutions = stations_country['county'].to_list()

    folder_location = 'C:\\Countries_SHP\\{}'.format(country)
    files = os.listdir(folder_location)
    shp_file_1 = [file for file in files if '_1.shp' in file]
    shp_file_1 = shp_file_1[0]
    shape_file_1_path = 'C:\\Countries_SHP\\{0}\\{1}'.format(country, shp_file_1)
    gdf_1 = gpd.read_file(shape_file_1_path)

    shape_file_2_path = 'C:\\GEOGloWS\v2\\all_boundaries_simplified.shp'
    gdf_2 = gpd.read_file(shape_file_2_path)

    for id, code, name, latitude, longitude, elevation, institution in zip(IDs, Codes, Names, Latitudes, Longitudes, Elevations, Institutions):

        print(code, ' - ', name, ' - ', latitude, ' - ', longitude, ' - ', elevation, 'm')

        point = Point(longitude, latitude)
        matching_feature_1 = gdf_1[gdf_1.geometry.contains(point)]
        matching_feature_2 = gdf_2[gdf_2.geometry.contains(point)]
        try:
            state = matching_feature_1['NAME_1'].values
            state = state[0]
            state = unidecode.unidecode(state)
        except Exception as e:
            state = '0'
            print(e)

        try:
            vpu = matching_feature_2['vpu_code'].values
            vpu = vpu[0]
        except Exception as e:
            vpu = '0'
            print(e)

        print (country, ' - ', state, ' - ', vpu)

        station_data.append([id, code, name, latitude, longitude, elevation, country, state, vpu])

station_data_df = pd.DataFrame(station_data, columns=['id', 'samplingFeatureCode', 'name', 'latitude', 'longitude', 'elevation_m', 'country', 'state', 'vpu'])
station_data_df.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama_Stations_State_VPU.csv")



