import geoglows
import statistics
import numpy as np
import pandas as pd
import hydrostats.data as hd
from scipy.stats import pearsonr

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Hydroweb_Stations.csv')

stations_pd = stations_pd[stations_pd['COMID_v1'] != 0]

COMIDs = stations_pd['COMID_v1'].tolist()
Codes = stations_pd['samplingFeatureCode'].tolist()
Names = stations_pd['name'].tolist()
Latitudes = stations_pd['latitude'].to_list()
Longitudes = stations_pd['longitude'].to_list()
Basins = stations_pd['Basin'].to_list()
Rivers = stations_pd['River'].to_list()
Regions = stations_pd['Region'].to_list()
VPUs = stations_pd['VPU'].to_list()

all_metrics = pd.DataFrame()

for code, name, comid, latitude, longitude, basin, river, region, vpu in zip(Codes, Names, COMIDs, Latitudes, Longitudes, Basins, Rivers, Regions, VPUs):

    print(name, ' - ', comid, ' - WL (Sat)')

    try:
        # Observed Data
        df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Observed_Hydroweb/{}.csv'.format(code), index_col=0)
        df.index = pd.to_datetime(df.index)
        observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
        observed_df.index = pd.to_datetime(observed_df.index)
        observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
        observed_df.index = pd.to_datetime(observed_df.index)

        # Corrected Data
        corrected_df = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/DWLT/GEOGLOWS_v1/{0}-{1}_WL.csv'.format(code, comid), index_col=0)
        corrected_df.index = pd.to_datetime(corrected_df.index)
        corrected_df.index = corrected_df.index.to_series().dt.strftime("%Y-%m-%d")
        corrected_df.index = pd.to_datetime(corrected_df.index)

        merged_df2 = hd.merge_data(obs_df=observed_df, sim_df=corrected_df)

        sim_array_2 = merged_df2.iloc[:, 0].values
        obs_array_2 = merged_df2.iloc[:, 1].values

        sim_mean_2 = statistics.mean(sim_array_2)
        obs_mean_2 = statistics.mean(obs_array_2)

        sim_std_2 = statistics.stdev(sim_array_2)
        obs_std_2 = statistics.stdev(obs_array_2)

        bias_2 = sim_mean_2 / obs_mean_2
        variability_2 = ((sim_std_2 / sim_mean_2) / (obs_std_2 / obs_mean_2))
        r_2, p_2 = pearsonr(obs_array_2, sim_array_2)
        kge_2 = 1 - ((r_2 - 1) ** 2 + (bias_2 - 1) ** 2 + (variability_2 - 1) ** 2) ** (1 / 2)

        # print(bias_2, ' - ', variability_2, ' - ', r_2, ' - ', kge_2)

        table_metrics = pd.DataFrame({
            'Code': [code],
            'Station': [name],
            'River': [river],
            'Basin': [basin],
            'Latitude': [latitude],
            'Longitude': [longitude],
            'COMID_v1': [comid],
            'Region': [region],
            'VPU': [vpu],
            'Bias': [bias_2],
            'Variability': [variability_2],
            'Correlation': [r_2],
            'KGE': [kge_2]
        })

        all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

    except Exception as e:
        print(e)

all_metrics.to_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Metrics_GEOGloWS_v1_sat_WL.csv', index=False)