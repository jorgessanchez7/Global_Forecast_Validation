import geoglows
import statistics
import numpy as np
import pandas as pd
import hydrostats.data as hd
from scipy.stats import pearsonr

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('/Users/grad/Github/Global_Forecast_Validation/Global/World_Stations_WL_Sat.csv')
#stations_pd = pd.read_csv('Metrics_GEOGloWS_v1_sat_WL_error_stations.csv')

COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()
Latitudes = stations_pd['Latitude'].to_list()
Longitudes = stations_pd['Longitude'].to_list()

all_metrics = pd.DataFrame()

for name, comid, latitude, longitude in zip(Names, COMIDs, Latitudes, Longitudes):

    print(name, ' - ', comid, ' - WL (Sat)')

    try:
        # Observed Data
        df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Hydroweb/Data/{}.csv'.format(name), index_col=0)
        df.index = pd.to_datetime(df.index)
        observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
        observed_df.index = pd.to_datetime(observed_df.index)
        observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
        observed_df.index = pd.to_datetime(observed_df.index)

        # Corrected Data
        corrected_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Corrected_Data/Hydroweb-GEOGLOWS_v1/{0}-{1}_WL.csv'.format(name, comid), index_col=0)
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
            'Station': [name],
            'Latitude': [latitude],
            'Longitude': [longitude],
            'COMID_v1': [comid],
            'Bias Corrected': [bias_2],
            'Corrected Variability': [variability_2],
            'Corrected Correlation': [r_2],
            'Corrected KGE': [kge_2]
        })

        all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

    except Exception as e:
        print(e)

all_metrics.to_csv('Metrics_GEOGloWS_v1_sat_WL.csv')