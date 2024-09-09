import geoglows
import statistics
import numpy as np
import pandas as pd
import hydrostats.data as hd
from scipy.stats import pearsonr

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('/Users/grad/Github/Global_Forecast_Validation/Global/World_Stations.csv')
stations_pd = stations_pd[stations_pd['Q'] == 'YES']

Folders = stations_pd['Folder'].tolist()
Sources = stations_pd['Data_Source'].tolist()
IDs = stations_pd['samplingFeatureCode'].tolist()
COMIDs = stations_pd['samplingFeatureType'].tolist()
COMID2s = stations_pd['COMID_v2'].tolist()
Names = stations_pd['name'].tolist()
Latitudes = stations_pd['latitude'].to_list()
Longitudes = stations_pd['longitude'].to_list()

all_metrics = pd.DataFrame()

for id, name, comid, comid2, latitude, longitude, folder, source in zip(IDs, Names, COMIDs, COMID2s, Latitudes, Longitudes, Folders, Sources):

    print(id, ' - ', name, ' - ', comid, ' - ', comid2, ' - ', latitude, ' - ', longitude)

    try:
        # Observed Data
        df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/{0}/{1}/{2}_Q.csv'.format(folder, source, id), na_values=-9999, index_col=0)
        df[df < 0] = np.nan
        df.index = pd.to_datetime(df.index)
        observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
        observed_df.index = pd.to_datetime(observed_df.index)
        observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
        observed_df.index = pd.to_datetime(observed_df.index)
    
        # Simulated Data
        simulated_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Simulated_Data/GEOGLOWS_v1/{}.csv'.format(comid), index_col=0)
        simulated_df[simulated_df < 0] = 0
        simulated_df.index = pd.to_datetime(simulated_df.index)
        simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
        simulated_df.index = pd.to_datetime(simulated_df.index)
    
        # Corrected Data
        corrected_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Corrected_Data/GEOGLOWS_v1/{0}-{1}_Q.csv'.format(id, comid), index_col=0)
        corrected_df[corrected_df < 0] = 0
        corrected_df.index = pd.to_datetime(corrected_df.index)
        corrected_df.index = corrected_df.index.to_series().dt.strftime("%Y-%m-%d")
        corrected_df.index = pd.to_datetime(corrected_df.index)
    
        merged_df = hd.merge_data(obs_df=observed_df, sim_df=simulated_df)
        merged_df2 = hd.merge_data(obs_df=observed_df, sim_df=corrected_df)
    
        sim_array = merged_df.iloc[:, 0].values
        obs_array = merged_df.iloc[:, 1].values
    
        sim_array_2 = merged_df2.iloc[:, 0].values
        obs_array_2 = merged_df2.iloc[:, 1].values
    
        sim_mean = statistics.mean(sim_array)
        obs_mean = statistics.mean(obs_array)
    
        sim_mean_2 = statistics.mean(sim_array_2)
        obs_mean_2 = statistics.mean(obs_array_2)
    
        sim_std = statistics.stdev(sim_array)
        obs_std = statistics.stdev(obs_array)
    
        sim_std_2 = statistics.stdev(sim_array_2)
        obs_std_2 = statistics.stdev(obs_array_2)
        
    
        bias = sim_mean / obs_mean
        variability = ((sim_std / sim_mean) / (obs_std / obs_mean))
        r, p = pearsonr(obs_array, sim_array)
        kge = 1 - ((r - 1) ** 2 + (bias - 1) ** 2 + (variability-1) ** 2) ** (1 / 2)

        #print(bias, ' - ', variability, ' - ', r, ' - ', kge)

        ###################################

        bias_2 = sim_mean_2 / obs_mean_2
        variability_2 = ((sim_std_2 / sim_mean_2) / (obs_std_2 / obs_mean_2))
        r_2, p_2 = pearsonr(obs_array_2, sim_array_2)
        kge_2 = 1 - ((r_2 - 1) ** 2 + (bias_2 - 1) ** 2 + (variability_2-1) ** 2) ** (1 / 2)

        #print(bias_2, ' - ', variability_2, ' - ', r_2, ' - ', kge_2)

        table_metrics = pd.DataFrame({
            'Folder': [folder],
            'Data_Source': [source],
            'Code': [id],
            'Name': [name],
            'Latitude': [latitude],
            'Longitude': [longitude],
            'COMID_v1': [comid],
            'COMID_v2': [comid2],
            'Bias': [bias],
            'Bias Corrected': [bias_2],
            'Variability': [variability],
            'Corrected Variability': [variability_2],
            'Correlation': [r],
            'Corrected Correlation': [r_2],
            'KGE': [kge],
            'Corrected KGE': [kge_2]
        })

        all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

    except Exception as e:
        print(e)

all_metrics.to_csv('Metrics_GEOGloWS_v1_Q.csv')