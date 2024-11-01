import geoglows
import statistics
import numpy as np
import pandas as pd
import HydroErr as he
import hydrostats.data as hd
from scipy.stats import pearsonr

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/Hydroweb/Common_stations_hydroweb_and_gauging_stations_v3.csv')

Hydroweb_Names = stations_pd['Station'].tolist()
Hydroweb_Latitudes = stations_pd['Hydroweb_Latitude'].tolist()
Hydroweb_Longitudes = stations_pd['Hydroweb_Longitude'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Station_Names = stations_pd['Station_Name'].tolist()
Station_Latitudes = stations_pd['Station_Latitude'].to_list()
Station_Longitudes = stations_pd['Station_Longitude'].to_list()
Countries = stations_pd['Country'].to_list()
Codes = stations_pd['ID'].to_list()
latitudes = stations_pd['Latitude'].to_list()
longitudes = stations_pd['Longitude'].to_list()

def adjust_water_levels(observed_ground_wl, observed_satellite_wl, units):

    if units == 'm':
      observed_ground_wl = observed_ground_wl.rename(columns={'Water Level (m)': 'Water Level'})

    # Merging the observed ground water levels and satellite water levels
    merged = hd.merge_data(obs_df=observed_ground_wl, sim_df=observed_satellite_wl)

    # Display initial merged data
    #display(HTML(merged.to_html()))

    # Convert observed ground water levels from cm to meters if necessary
    if units == 'cm':
      merged['Observed'] = merged['Observed'] / 100  # Converting cm to meters
      #display(HTML(merged.to_html()))  # Display after conversion

    # Prepare arrays for MAE calculation
    obs_array = merged.iloc[:, 1].values
    sim_array = merged.iloc[:, 0].values

    # Calculate adjustments using MAE
    a = np.max(sim_array) - np.min(obs_array)
    b = np.min(sim_array) - np.max(obs_array)
    z0 = np.random.uniform(low=np.minimum(a, b), high=np.maximum(a, b), size=(100000,))

    MAE = [[he.mae(sim_array, obs_array + z), z] for z in z0]
    MAE_df = pd.DataFrame(MAE, columns=['MAE', 'Z'])

    # Find the adjustment with the minimum MAE
    z_min = MAE_df.loc[MAE_df['MAE'].idxmin(), 'Z']

    # Apply the adjustment
    merged['Observed'] = merged['Observed'] + z_min

    return merged, z_min

all_metrics = pd.DataFrame()

for hydroweb_name, hydroweb_latitude, hydroweb_longitude, comid, station_name, station_latitude, station_longitude, country, code, latitude, longitude in zip(Hydroweb_Names, Hydroweb_Latitudes, Hydroweb_Longitudes, COMIDs, Station_Names, Station_Latitudes, Station_Longitudes, Countries, Codes, latitudes, longitudes):

    print(hydroweb_name, ' - ', hydroweb_latitude, ' - ', hydroweb_longitude, ' - ', comid)
    print(code, ' - ', station_name, ' - ', station_latitude, ' - ', station_longitude, ' - ', comid)
    #print(latitude, ' - ', longitude)
    
    # Observed Data
    observed_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/GEOGloWS_Training_Materials/Observed_Data/{0}/{1}_WL.csv'.format(country, code), index_col=0) 
    observed_df.index = pd.to_datetime(observed_df.index)
    observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
    observed_df.index = pd.to_datetime(observed_df.index)

    units = observed_df.columns[0]
    if units == 'Water Level (m)':
        units = 'm'
    elif units == 'Water Level (cm)':
        units = 'cm'
    else:
        print('Check Water Level File!')
    
    # Satellite Data
    satellite_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/GEOGloWS_Training_Materials/Observed_Data/Hydroweb/{0}.csv'.format(hydroweb_name), index_col=0)
    satellite_df.index = pd.to_datetime(satellite_df.index)
    satellite_df.index = satellite_df.index.to_series().dt.strftime("%Y-%m-%d")
    satellite_df.index = pd.to_datetime(satellite_df.index)

    merged_df2, z_0 = adjust_water_levels(observed_df, satellite_df, units)
    
    sim_array_2 = merged_df2.iloc[:, 0].values
    obs_array_2 = merged_df2.iloc[:, 1].values

    sim_mean_2 = statistics.mean(sim_array_2)
    obs_mean_2 = statistics.mean(obs_array_2)

    sim_std_2 = statistics.stdev(sim_array_2)
    obs_std_2 = statistics.stdev(obs_array_2)

    bias_2 = sim_mean_2 / obs_mean_2
    variability_2 = ((sim_std_2 / sim_mean_2) / (obs_std_2 / obs_mean_2))
    r_2, p_2 = pearsonr(obs_array_2, sim_array_2)
    kge_2 = 1 - ((r_2 - 1) ** 2 + (bias_2 - 1) ** 2 + (variability_2-1) ** 2) ** (1 / 2)

    #print(bias_2, ' - ', variability_2, ' - ', r_2, ' - ', kge_2)

    table_metrics = pd.DataFrame({
        'COMID': [comid],
        'Station': [hydroweb_name],
        'Hydroweb_Latitude': [hydroweb_latitude],
        'Hydroweb_Longitude': [hydroweb_longitude],
        'Station_Name': [station_name],
        'Station_Latitude': [station_latitude],
        'Station_Longitude': [station_longitude],
        'Country': [country],
        'ID': [code],
        'Latitude': [latitude],
        'Longitude': [longitude],
        'Zo (m)': [z_0],
        'Bias': [bias_2],
        'Variability': [variability_2],
        'Correlation': [r_2],
        'KGE': [kge_2]
    })

    all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

all_metrics.to_csv('Metrics_Sat_vs_Grouund_WL.csv')