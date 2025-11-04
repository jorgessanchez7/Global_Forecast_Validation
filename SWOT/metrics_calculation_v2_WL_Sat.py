import geoglows
import statistics
import numpy as np
import pandas as pd
import hydrostats.data as hd
from scipy.stats import pearsonr

import warnings
warnings.filterwarnings('ignore')

stations_pd = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\SWOT_Stations.csv')

IDs = stations_pd['reach_id'].tolist()
COMIDs = stations_pd['COMID_v1'].tolist()
COMID2s = stations_pd['COMID_v2'].tolist()

all_metrics = pd.DataFrame()

for id, comid, comid2 in zip(IDs, COMIDs, COMID2s):

    print(id, ' - ', comid, ' - ', comid2, ' - WL')

    try:
        # Observed Data
        df = pd.read_csv('E:\\GEOGloWS\\04_Observed_Satellite_Water_Levels\\SWOT\\{0}.csv'.format(id), index_col=0)
        df[df < 0] = np.nan
        df.index = pd.to_datetime(df.index)
        observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
        observed_df.index = pd.to_datetime(observed_df.index)
        observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
        observed_df.index = pd.to_datetime(observed_df.index)
    
        # Corrected Data
        corrected_df = pd.read_csv('E:\\GEOGloWS\\03_Transformed_Data\\swot_v2\\{0}-{1}_WL.csv'.format(id, comid2), index_col=0)
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
        kge_2 = 1 - ((r_2 - 1) ** 2 + (bias_2 - 1) ** 2 + (variability_2-1) ** 2) ** (1 / 2)

        #print(bias_2, ' - ', variability_2, ' - ', r_2, ' - ', kge_2)

        table_metrics = pd.DataFrame({
            'ID': [id],
            'COMID_v1': [comid],
            'COMID_v2': [comid2],
            'Bias Corrected': [bias_2],
            'Corrected Variability': [variability_2],
            'Corrected Correlation': [r_2],
            'Corrected KGE': [kge_2]
        })

        all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

    except Exception as e:
        print(e)

all_metrics.to_csv('E:\\GEOGloWS\\Error_Metrics\\Metrics\\Metrics_GEOGloWS_v2_WL_SWOT.csv')