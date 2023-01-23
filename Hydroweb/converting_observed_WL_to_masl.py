import numpy as np
import pandas as pd
import HydroErr as he
import hydrostats.data as hd

common_comid_list = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/Common_stations_hydroweb_and_gauging_stations_v1.csv')

IDs = common_comid_list['ID'].tolist()
COMIDs = common_comid_list['COMID'].tolist()
stations = common_comid_list['Station'].tolist()
Names = common_comid_list['Station_Name'].tolist()
Countries = common_comid_list['Country'].tolist()
Regions = common_comid_list['Region'].tolist()

Z_array = []

for id, comid, name, station, country, region in zip(IDs, COMIDs, Names, stations, Countries, Regions):

    print(id, '-', comid, '-', name, '-', station, '-', country, '-', region)

    observed_wl = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Observed_Data_WL/{2}.csv'.format(region, country, id), index_col=0)
    observed_wl.index = pd.to_datetime(observed_wl.index)
    observed_wl = observed_wl.groupby(observed_wl.index.strftime("%Y-%m-%d")).mean()
    observed_wl.index = pd.to_datetime(observed_wl.index)

    corrected_wl = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Corrected_Data_WL/{2}-{3}.csv'.format(region, country, id, comid), index_col=0)

    sensor_wl_max = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/max/{}_max.csv'.format(station), index_col=0)
    sensor_wl_max.index = pd.to_datetime(sensor_wl_max.index)
    sensor_wl_max = sensor_wl_max.groupby(sensor_wl_max.index.strftime("%Y-%m-%d")).mean()
    sensor_wl_max.index = pd.to_datetime(sensor_wl_max.index)
    sensor_wl_max.rename(columns={'Water Level (m)': 'Water Level (msnm)'}, inplace=True)

    sensor_wl_mean = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/mean/{}_mean.csv'.format(station), index_col=0)
    sensor_wl_mean.index = pd.to_datetime(sensor_wl_mean.index)
    sensor_wl_mean = sensor_wl_mean.groupby(sensor_wl_mean.index.strftime("%Y-%m-%d")).mean()
    sensor_wl_mean.index = pd.to_datetime(sensor_wl_mean.index)
    sensor_wl_mean.rename(columns={'Water Level (m)': 'Water Level (msnm)'}, inplace=True)

    sensor_wl_min = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Observed_Data/min/{}_min.csv'.format(station), index_col=0)
    sensor_wl_min.index = pd.to_datetime(sensor_wl_min.index)
    sensor_wl_min = sensor_wl_min.groupby(sensor_wl_min.index.strftime("%Y-%m-%d")).mean()
    sensor_wl_min.index = pd.to_datetime(sensor_wl_min.index)
    sensor_wl_min.rename(columns={'Water Level (m)': 'Water Level (msnm)'}, inplace=True)

    merged_df_max = hd.merge_data(obs_df=observed_wl, sim_df=sensor_wl_max)
    merged_df_mean = hd.merge_data(obs_df=observed_wl, sim_df=sensor_wl_mean)
    merged_df_min = hd.merge_data(obs_df=observed_wl, sim_df=sensor_wl_min)

    obs_array = merged_df_mean.iloc[:, 1].values

    sim_array_max = merged_df_max.iloc[:, 0].values
    sim_array_mean = merged_df_mean.iloc[:, 0].values
    sim_array_min = merged_df_min.iloc[:, 0].values

    if observed_wl.columns[0] == 'Water Level (m)':

        merged_df_max.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/max/{0}_{1}_{2}_max.csv'.format(comid, id, station))
        merged_df_mean.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/mean/{0}_{1}_{2}_mean.csv'.format(comid, id, station))
        merged_df_min.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/min/{0}_{1}_{2}_min.csv'.format(comid, id, station))

        # create a excel writer object
        with pd.ExcelWriter("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/Excel/{0}_{1}_{2}.xlsx".format(comid, id, station)) as writer:

            # use to_excel function and specify the sheet_name and index
            # to store the dataframe in specified sheet
            merged_df_max.to_excel(writer, sheet_name="max", index=False)
            merged_df_mean.to_excel(writer, sheet_name="mean", index=False)
            merged_df_min.to_excel(writer, sheet_name="min", index=False)

        # a = np.max(sim_array_max) - np.min(obs_array)
        # b = np.min(sim_array_min) - np.max(obs_array)
        #
        # z0 = np.random.uniform(low=np.minimum(a,b), high=np.maximum(a,b), size=(1000000,))
        #
        # MAE_max = []
        # MAE_mean = []
        # MAE_min = []
        #
        # for z in z0:
        #
        #     MAE_max.append([he.mae(sim_array_max, obs_array+z), z])
        #     MAE_mean.append([he.mae(sim_array_mean, obs_array + z), z])
        #     MAE_min.append([he.mae(sim_array_min, obs_array + z), z])
        #
        # MAE_max_df = pd.DataFrame(MAE_max, columns=['MAE_max', 'Z max'])
        # MAE_mean_df = pd.DataFrame(MAE_mean, columns=['MAE_mean', 'Z mean'])
        # MAE_min_df = pd.DataFrame(MAE_min, columns=['MAE_min', 'Z min'])
        #
        # z_max_df = MAE_max_df[MAE_max_df['MAE_max'] == MAE_max_df['MAE_max'].min()]
        # z_mean_df = MAE_mean_df[MAE_mean_df['MAE_mean'] == MAE_mean_df['MAE_mean'].min()]
        # z_min_df = MAE_min_df[MAE_min_df['MAE_min'] == MAE_min_df['MAE_min'].min()]
        #
        # Z_array.append([id, comid, name, station, country, region, z_max_df.iloc[0]['Z max'], z_max_df.iloc[0]['MAE_max'], z_mean_df.iloc[0]['Z mean'], z_mean_df.iloc[0]['MAE_mean'], z_min_df.iloc[0]['Z min'], z_min_df.iloc[0]['MAE_min']])
        #
        # observed_masl_max = observed_wl['Water Level (m)'] + z_max_df.iloc[0]['Z max']
        # observed_masl_max.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Observed_Data_WL/max/{0}_{1}_max.csv'.format(id, station))
        #
        # observed_masl_mean = observed_wl['Water Level (m)'] + z_mean_df.iloc[0]['Z mean']
        # observed_masl_mean.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Observed_Data_WL/mean/{0}_{1}_mean.csv'.format(id, station))
        #
        # observed_masl_min = observed_wl['Water Level (m)'] + z_min_df.iloc[0]['Z min']
        # observed_masl_min.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Observed_Data_WL/min/{0}_{1}_min.csv'.format(id, station))
        #
        # corrected_masl_max = corrected_wl['Simulated Water Level (m)'] + z_max_df.iloc[0]['Z max']
        # corrected_masl_max.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Corrected_Data_WL/max/{0}-{1}_{2}_max.csv'.format(id, comid, station))
        #
        # corrected_masl_mean = corrected_wl['Simulated Water Level (m)'] + z_mean_df.iloc[0]['Z mean']
        # corrected_masl_mean.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Corrected_Data_WL/mean/{0}-{1}_{2}_mean.csv'.format(id, comid, station))
        #
        # corrected_masl_min = corrected_wl['Simulated Water Level (m)'] + z_min_df.iloc[0]['Z min']
        # corrected_masl_min.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Corrected_Data_WL/min/{0}-{1}_{2}_min.csv'.format(id, comid, station))

    elif observed_wl.columns[0] == 'Water Level (cm)':

        # a = np.max(sim_array_max) - (np.min(obs_array)/100)
        # b = np.min(sim_array_min) - (np.max(obs_array)/100)
        #
        # z0 = np.random.uniform(low=np.minimum(a,b), high=np.maximum(a,b), size=(1000000,))
        #
        # MAE_max = []
        # MAE_mean = []
        # MAE_min = []
        #
        # for z in z0:
        #     MAE_max.append([he.mae(sim_array_max, (obs_array/100) + z), z])
        #     MAE_mean.append([he.mae(sim_array_mean, (obs_array/100) + z), z])
        #     MAE_min.append([he.mae(sim_array_min, (obs_array/100) + z), z])
        #
        # MAE_max_df = pd.DataFrame(MAE_max, columns=['MAE_max', 'Z max'])
        # MAE_mean_df = pd.DataFrame(MAE_mean, columns=['MAE_mean', 'Z mean'])
        # MAE_min_df = pd.DataFrame(MAE_min, columns=['MAE_min', 'Z min'])
        #
        # z_max_df = MAE_max_df[MAE_max_df['MAE_max'] == MAE_max_df['MAE_max'].min()]
        # z_mean_df = MAE_mean_df[MAE_mean_df['MAE_mean'] == MAE_mean_df['MAE_mean'].min()]
        # z_min_df = MAE_min_df[MAE_min_df['MAE_min'] == MAE_min_df['MAE_min'].min()]
        #
        # Z_array.append([id, comid, name, station, country, region, z_max_df.iloc[0]['Z max'], z_max_df.iloc[0]['MAE_max'], z_mean_df.iloc[0]['Z mean'], z_mean_df.iloc[0]['MAE_mean'], z_min_df.iloc[0]['Z min'], z_min_df.iloc[0]['MAE_min']])

        merged_df_max['Observed'] = (merged_df_max['Observed'] / 100)
        merged_df_mean['Observed'] = (merged_df_mean['Observed'] / 100)
        merged_df_min['Observed'] = (merged_df_min['Observed'] / 100)

        merged_df_max.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/max/{0}_{1}_{2}_max.csv'.format(comid, id, station))
        merged_df_mean.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/mean/{0}_{1}_{2}_mean.csv'.format(comid, id, station))
        merged_df_min.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/min/{0}_{1}_{2}_min.csv'.format(comid, id, station))

        # create a excel writer object
        with pd.ExcelWriter("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Merged_Data/Excel/{0}_{1}_{2}.xlsx".format(comid, id, station)) as writer:

            # use to_excel function and specify the sheet_name and index
            # to store the dataframe in specified sheet
            merged_df_max.to_excel(writer, sheet_name="max", index=True)
            merged_df_mean.to_excel(writer, sheet_name="mean", index=True)
            merged_df_min.to_excel(writer, sheet_name="min", index=True)

    #     observed_masl_max = ((observed_wl['Water Level (cm)']/100) + z_max_df.iloc[0]['Z max']).to_frame()
    #     observed_masl_max.rename(columns={'Water Level (cm)': 'Water Level (m)'}, inplace=True)
    #     observed_masl_max.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Observed_Data_WL/max/{0}_{1}_max.csv'.format(id, station))
    #
    #     observed_masl_mean = ((observed_wl['Water Level (cm)']/100) + z_mean_df.iloc[0]['Z mean']).to_frame()
    #     observed_masl_mean.rename(columns={'Water Level (cm)': 'Water Level (m)'}, inplace=True)
    #     observed_masl_mean.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Observed_Data_WL/mean/{0}_{1}_mean.csv'.format(id, station))
    #
    #     observed_masl_min = ((observed_wl['Water Level (cm)']/100) + z_min_df.iloc[0]['Z min']).to_frame()
    #     observed_masl_min.rename(columns={'Water Level (cm)': 'Water Level (m)'}, inplace=True)
    #     observed_masl_min.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Observed_Data_WL/min/{0}_{1}_min.csv'.format(id, station))
    #
    #     corrected_masl_max = ((corrected_wl['Simulated Water Level (cm)']/100) + z_max_df.iloc[0]['Z max']).to_frame()
    #     corrected_masl_max.rename(columns={'Simulated Water Level (cm)': 'Simulated Water Level (m)'}, inplace=True)
    #     corrected_masl_max.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Corrected_Data_WL/max/{0}-{1}_{2}_max.csv'.format(id, comid, station))
    #
    #     corrected_masl_mean = ((corrected_wl['Simulated Water Level (cm)']/100) + z_mean_df.iloc[0]['Z mean']).to_frame()
    #     corrected_masl_mean.rename(columns={'Simulated Water Level (cm)': 'Simulated Water Level (m)'}, inplace=True)
    #     corrected_masl_mean.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Corrected_Data_WL/mean/{0}-{1}_{2}_mean.csv'.format(id, comid, station))
    #
    #     corrected_masl_min = ((corrected_wl['Simulated Water Level (cm)']/100) + z_min_df.iloc[0]['Z min']).to_frame()
    #     corrected_masl_min.rename(columns={'Simulated Water Level (cm)': 'Simulated Water Level (m)'}, inplace=True)
    #     corrected_masl_min.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Corrected_Data_WL/min/{0}-{1}_{2}_min.csv'.format(id, comid, station))
    #
    # Z_array_df = pd.DataFrame(Z_array, columns=['ID', 'COMID', 'Station_Name', 'Station', 'Country', 'Region', 'Z max', 'Error MAX', 'Z mean', 'Error mean', 'Z min', 'Error min'])
    # Z_array_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/Zero_Level.csv')




