import os
import re
import math
import shutil
import logging
import pandas as pd

from eodag import setup_logging
from eodag import EODataAccessGateway, SearchResult
from eodag.plugins.download.http import HTTPDownload
from eodag.plugins.authentication.header import HTTPHeaderAuth

import warnings
warnings.filterwarnings('ignore')


def fetch_water_level_data(api_key, latitude, longitude, satellite_station, productType):
    # Setup environment
    os.environ["EODAG__HYDROWEB_NEXT__AUTH__CREDENTIALS__APIKEY"] = api_key
    setup_logging(1)  # Set logging to only show progress bars
    custom_logger = logging.getLogger("Hydroweb.next")
    custom_logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)-15s %(name)-32s [%(levelname)-8s] %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    custom_logger.addHandler(handler)
    os.environ["EODAG__HYDROWEB_NEXT__SEARCH__TIMEOUT"] = "30"
    dag = EODataAccessGateway()

    # Configure download path
    # path_out = os.getcwd()
    # path_out = '/tmp'
    path_out = '/var/folders/5x/j7c2788n7559tmz5ztq86k280000gp/T'
    if not os.path.isdir(path_out):
        raise RuntimeError(f"Path {path_out} does not exist")

    # Calculate bounding box from coordinates
    earth_radius = 111320  # Earth's radius in meters at the equator
    delta_lat = 0.5 / earth_radius
    delta_lon = 0.5 / (earth_radius * math.cos(math.radians(latitude)))
    bl_lon = longitude - delta_lon
    bl_lat = latitude - delta_lat
    br_lon = longitude + delta_lon
    br_lat = latitude - delta_lat
    tr_lon = longitude + delta_lon
    tr_lat = latitude + delta_lat
    tl_lon = longitude - delta_lon
    tl_lat = latitude + delta_lat
    wkt_polygon = f"POLYGON (({bl_lon} {bl_lat}, {br_lon} {br_lat}, {tr_lon} {tr_lat}, {tl_lon} {tl_lat}, {bl_lon} {bl_lat}))"

    if productType == "Research":
        # Setup query arguments
        query_args = {
            "productType": "HYDROWEB_RIVERS_RESEARCH",
            "geom": wkt_polygon,
            "items_per_page": 2000
        }
    elif productType == "Operational":
        # Setup query arguments
        query_args = {
            "productType": "HYDROWEB_RIVERS_OPE",
            "geom": wkt_polygon,
            "items_per_page": 2000
        }

    # Search for products
    search_results = []
    for i, page_results in enumerate(dag.search_iter_page(**query_args)):
        search_results.extend(page_results)

    # Download products
    downloaded_paths = dag.download_all(search_results, outputs_prefix=path_out)
    # print("Downloaded data paths:", downloaded_paths)  # Add this line

    # Process the downloaded data
    station_name = satellite_station.upper()
    file_name = f"hydroprd_R_{station_name}_exp.txt"
    print(file_name)
    pattern = r"R_.+-R_.+_hysope2_.+"
    entries = os.listdir('/var/folders/5x/j7c2788n7559tmz5ztq86k280000gp/T/')
    folders = []
    for entry in entries:
        entry_path = os.path.join(path_out, entry)
        if re.match(pattern, entry) and os.path.isdir(entry_path):
            folders.append(entry_path)

    selected_folder = None

    for folder in folders:
        if os.path.exists(os.path.join(folder, file_name)):
            selected_folder = folder

    if selected_folder is None:
        print("No folder contains the target file.")
        return None
    else:
        for folder in folders:
            if folder != selected_folder:
                shutil.rmtree(folder)

    folders = [selected_folder]

    if folders:
        folders = folders[0]
        file_location = f"{folders}/hydroprd_R_{station_name}_exp.txt"

        skip = 40

        while True:
            try:
                data = pd.read_csv(file_location, skiprows=skip, sep=' ', header=None, index_col=0)
                data.index = pd.to_datetime(data.index)
                data.index = data.index.to_series().dt.strftime("%Y-%m-%d")
                data.index = pd.to_datetime(data.index)
                data = data[[col for col in data.columns if col <= 2]]
                data = data.drop(columns=1, axis=1)
                data.index.name = 'Datetime'
                data.rename(columns={2: 'Water Level (m)'}, inplace=True)
                break
            except Exception as e:
                skip = skip + 1
                continue

        os.remove(f"{folders}/hydroprd_R_{station_name}_exp.txt")
        os.rmdir(f"{folders}")
        return data
    else:
        return None

stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Hydroweb/Total_Rivers_Stations_Hydroweb_Hydroserver_v3.csv')

codes = stations_pd['samplingFeatureCode'].tolist()
names = stations_pd['name'].tolist()
latitudes = stations_pd['latitude'].tolist()
longitudes = stations_pd['longitude'].tolist()
vpus = stations_pd['VPU'].tolist()
types = stations_pd['Type'].tolist()

#Global variables
api_key = "kFb4XBaZX7d4RnWccF4jJugGQpAyeWvbLS1u09ngrHBqEF4gco"

for code, name, latitude, longitude, vpu, type in zip(codes,names, latitudes, longitudes, vpus, types):

    print(code, ' - ', name, ' - ', latitude, ' - ', longitude, ' - ', vpu, ' - ', type)

    try:
        observed_satellite_wl = fetch_water_level_data(api_key, latitude, longitude, name, type)
        observed_satellite_wl.index = pd.to_datetime(observed_satellite_wl.index)
        observed_satellite_wl.index = observed_satellite_wl.index.to_series().dt.strftime("%Y-%m-%d")
        observed_satellite_wl.index = pd.to_datetime(observed_satellite_wl.index)

        #observed_satellite_wl.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Hydroweb/Data/{}.csv'.format(name))
        #observed_satellite_wl.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Hydroweb/Observed_Data/{}.csv'.format(code))

        observed_satellite_wl.to_csv('/Users/grad/Documents/Hydrological_Data/Hydroweb/{}.csv'.format(code))

        #if vpu == 122:
        #    observed_satellite_wl.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/NBI/Observed_Data/{}.csv'.format(code))

    except Exception as e:
        print(e)