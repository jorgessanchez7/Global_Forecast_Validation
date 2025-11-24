import geoglows
import statistics
import numpy as np
import pandas as pd
import hydrostats.data as hd
from scipy.stats import pearsonr

import warnings
warnings.filterwarnings('ignore')

stations_all = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\World_Stations.csv')
stations_all = stations_all[stations_all['samplingFeatureType'] != 0]
stations_all = stations_all[stations_all['Q'] == 'YES']

#regions = ['africa', 'australia', 'central_america', 'central_asia', 'east_asia-geoglows', 'europe', 'islands', 'japan',
#           'middle_east', 'north_america', 'south_america', 'south_asia', 'west_asia']

regions = ['central_asia', 'east_asia', 'europe', 'islands', 'japan',
           'middle_east', 'north_america', 'south_america', 'south_asia', 'west_asia']

for region in regions:

    stations_pd = stations_all[stations_all['GEOGloWS_v1_region'] == '{0}'.format(region)]

    Folders = stations_pd['Folder'].tolist()
    Sources = stations_pd['Data_Source'].tolist()
    IDs = stations_pd['samplingFeatureCode'].tolist()
    COMIDs = stations_pd['samplingFeatureType'].tolist()
    COMID2s = stations_pd['COMID_v2'].tolist()
    Names = stations_pd['name'].tolist()
    Latitudes = stations_pd['latitude'].to_list()
    Longitudes = stations_pd['longitude'].to_list()


    #Comparison 1. GEOGLOWS v1 by Juseth vs. GEOGLOWS v1 Original

    all_metrics = pd.DataFrame()

    for id, name, comid, comid2, latitude, longitude, folder, source in zip(IDs, Names, COMIDs, COMID2s, Latitudes, Longitudes, Folders, Sources):

        print(region, ' - ', id, ' - ', name, ' - ', comid, ' - ', comid2, ' - ', latitude, ' - ', longitude)

        try:
            # Observed Data (GEOGLOWS v1 Original)
            observed_df = pd.read_csv('E:\\GEOGloWS\\01_Simulated_Values\\v1\\{}.csv'.format(comid), index_col=0)
            observed_df[observed_df < 0] = 0
            observed_df.index = pd.to_datetime(observed_df.index)
            observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
            observed_df.index = pd.to_datetime(observed_df.index)

            # Simulated Data
            simulated_df = pd.read_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Historical_Simulation_or\\{}_or.csv'.format(comid), index_col=0)
            simulated_df[simulated_df < 0] = 0
            simulated_df.index = pd.to_datetime(simulated_df.index)
            simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
            simulated_df.index = pd.to_datetime(simulated_df.index)

            merged_df = hd.merge_data(obs_df=observed_df, sim_df=simulated_df)

            sim_array = merged_df.iloc[:, 0].values
            obs_array = merged_df.iloc[:, 1].values

            sim_mean = statistics.mean(sim_array)
            obs_mean = statistics.mean(obs_array)

            sim_std = statistics.stdev(sim_array)
            obs_std = statistics.stdev(obs_array)

            bias = sim_mean / obs_mean
            variability = ((sim_std / sim_mean) / (obs_std / obs_mean))
            r, p = pearsonr(obs_array, sim_array)
            kge = 1 - ((r - 1) ** 2 + (bias - 1) ** 2 + (variability-1) ** 2) ** (1 / 2)

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
                'Variability': [variability],
                'Correlation': [r],
                'KGE': [kge],
            })

            all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

        except Exception as e:
            print(e)

    all_metrics.dropna(subset=['KGE'], inplace=True)
    all_metrics.to_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Error_Metrics\\{0}-geoglows\\Metrics_GEOGLOWS_v1_Comparisons.csv'.format(region))


    #Comparison 2. GEOGLOWS v1 (by Juseth) vs. Observed Data

    all_metrics = pd.DataFrame()

    for id, name, comid, comid2, latitude, longitude, folder, source in zip(IDs, Names, COMIDs, COMID2s, Latitudes, Longitudes, Folders, Sources):

        print(region, ' - ', id, ' - ', name, ' - ', comid, ' - ', comid2, ' - ', latitude, ' - ', longitude)

        try:
            # Observed Data
            df = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder, source, id), na_values=-9999, index_col=0)
            df[df < 0] = np.nan
            df.index = pd.to_datetime(df.index)
            observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
            observed_df.index = pd.to_datetime(observed_df.index)
            observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
            observed_df.index = pd.to_datetime(observed_df.index)

            if observed_df.columns[0] == 'Streamflow (m3/s)':
                observed_df = observed_df.rename(columns={'Streamflow (m3/s)': 'Observed Streamflow (m3/s)'})

            # Simulated Data
            simulated_df = pd.read_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Historical_Simulation_or\\{}_or.csv'.format(comid), index_col=0)
            simulated_df[simulated_df < 0] = 0
            simulated_df.index = pd.to_datetime(simulated_df.index)
            simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
            simulated_df.index = pd.to_datetime(simulated_df.index)

            if simulated_df.columns[0] == 'Streamflow (m3/s)':
                simulated_df = simulated_df.rename(columns={'Streamflow (m3/s)': 'Simulated Streamflow (m3/s)'})

            merged_df = hd.merge_data(obs_df=observed_df, sim_df=simulated_df)

            sim_array = merged_df.iloc[:, 0].values
            obs_array = merged_df.iloc[:, 1].values

            sim_mean = statistics.mean(sim_array)
            obs_mean = statistics.mean(obs_array)

            sim_std = statistics.stdev(sim_array)
            obs_std = statistics.stdev(obs_array)

            bias = sim_mean / obs_mean
            variability = ((sim_std / sim_mean) / (obs_std / obs_mean))
            r, p = pearsonr(obs_array, sim_array)
            kge = 1 - ((r - 1) ** 2 + (bias - 1) ** 2 + (variability - 1) ** 2) ** (1 / 2)

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
                'Variability': [variability],
                'Correlation': [r],
                'KGE': [kge],
            })

            all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

        except Exception as e:
            print(e)

    all_metrics.dropna(subset=['KGE'], inplace=True)
    all_metrics.to_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Error_Metrics\\{0}-geoglows\\Metrics_GEOGloWS_v1_Q.csv'.format(region))


    #Comparison 3. GEOGLOWS v1 (Runoff Bias Correction) vs. Observed Data

    all_metrics = pd.DataFrame()

    for id, name, comid, comid2, latitude, longitude, folder, source in zip(IDs, Names, COMIDs, COMID2s, Latitudes, Longitudes, Folders, Sources):

        print(region, ' - ', id, ' - ', name, ' - ', comid, ' - ', comid2, ' - ', latitude, ' - ', longitude)

        try:
            # Observed Data
            df = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder, source, id), na_values=-9999, index_col=0)
            df[df < 0] = np.nan
            df.index = pd.to_datetime(df.index)
            observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
            observed_df.index = pd.to_datetime(observed_df.index)
            observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
            observed_df.index = pd.to_datetime(observed_df.index)

            if observed_df.columns[0] == 'Streamflow (m3/s)':
                observed_df = observed_df.rename(columns={'Streamflow (m3/s)': 'Observed Streamflow (m3/s)'})

            # Simulated Data
            simulated_df = pd.read_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Historical_Simulation_bc\\{}_bc.csv'.format(comid), index_col=0)
            simulated_df[simulated_df < 0] = 0
            simulated_df.index = pd.to_datetime(simulated_df.index)
            simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
            simulated_df.index = pd.to_datetime(simulated_df.index)

            if simulated_df.columns[0] == 'Streamflow (m3/s)':
                simulated_df = simulated_df.rename(columns={'Streamflow (m3/s)': 'Simulated Streamflow (m3/s)'})

            merged_df = hd.merge_data(obs_df=observed_df, sim_df=simulated_df)

            sim_array = merged_df.iloc[:, 0].values
            obs_array = merged_df.iloc[:, 1].values

            sim_mean = statistics.mean(sim_array)
            obs_mean = statistics.mean(obs_array)

            sim_std = statistics.stdev(sim_array)
            obs_std = statistics.stdev(obs_array)

            bias = sim_mean / obs_mean
            variability = ((sim_std / sim_mean) / (obs_std / obs_mean))
            r, p = pearsonr(obs_array, sim_array)
            kge = 1 - ((r - 1) ** 2 + (bias - 1) ** 2 + (variability - 1) ** 2) ** (1 / 2)

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
                'Variability': [variability],
                'Correlation': [r],
                'KGE': [kge],
            })

            all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

        except Exception as e:
            print(e)

    all_metrics.dropna(subset=['KGE'], inplace=True)
    all_metrics.to_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Error_Metrics\\{0}-geoglows\\Metrics_GEOGloWS_v1_RBC_Q.csv'.format(region))


    #Comparison 4. GEOGLOWS v1 (MFDC-QM) vs. Observed Data

    all_metrics = pd.DataFrame()

    for id, name, comid, comid2, latitude, longitude, folder, source in zip(IDs, Names, COMIDs, COMID2s, Latitudes, Longitudes, Folders, Sources):

        print(region, ' - ', id, ' - ', name, ' - ', comid, ' - ', comid2, ' - ', latitude, ' - ', longitude)

        try:
            # Observed Data
            df = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Global_Hydroserver\\Observed_Data\\{0}\\{1}\\{2}_Q.csv'.format(folder, source, id), na_values=-9999, index_col=0)
            df[df < 0] = np.nan
            df.index = pd.to_datetime(df.index)
            observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
            observed_df.index = pd.to_datetime(observed_df.index)
            observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
            observed_df.index = pd.to_datetime(observed_df.index)

            if observed_df.columns[0] == 'Streamflow (m3/s)':
                observed_df = observed_df.rename(columns={'Streamflow (m3/s)': 'Observed Streamflow (m3/s)'})

            # Simulated Data
            simulated_df = pd.read_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Historical_Simulation_MFDC-QM\\{0}-{1}_Q.csv'.format(id, comid), index_col=0)
            simulated_df[simulated_df < 0] = 0
            simulated_df.index = pd.to_datetime(simulated_df.index)
            simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
            simulated_df.index = pd.to_datetime(simulated_df.index)

            if simulated_df.columns[0] == 'Streamflow (m3/s)':
                simulated_df = simulated_df.rename(columns={'Streamflow (m3/s)': 'Simulated Streamflow (m3/s)'})

            merged_df = hd.merge_data(obs_df=observed_df, sim_df=simulated_df)

            sim_array = merged_df.iloc[:, 0].values
            obs_array = merged_df.iloc[:, 1].values

            sim_mean = statistics.mean(sim_array)
            obs_mean = statistics.mean(obs_array)

            sim_std = statistics.stdev(sim_array)
            obs_std = statistics.stdev(obs_array)

            bias = sim_mean / obs_mean
            variability = ((sim_std / sim_mean) / (obs_std / obs_mean))
            r, p = pearsonr(obs_array, sim_array)
            kge = 1 - ((r - 1) ** 2 + (bias - 1) ** 2 + (variability - 1) ** 2) ** (1 / 2)

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
                'Variability': [variability],
                'Correlation': [r],
                'KGE': [kge],
            })

            all_metrics = pd.concat([all_metrics, table_metrics], ignore_index=True)

        except Exception as e:
            print(e)

    all_metrics.dropna(subset=['KGE'], inplace=True)
    all_metrics.to_csv('E:\\Post_Doc\\GEOGLOWS_Applications\\Runoff_Bias_Correction\\GEOGLOWS_v1\\Error_Metrics\\{0}-geoglows\\Metrics_GEOGloWS_v1_MFDC-QM_Q.csv'.format(region))

