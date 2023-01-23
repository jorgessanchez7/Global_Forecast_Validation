import statistics
import pandas as pd
import hydrostats as hs
import hydrostats.data as hd

import warnings
warnings.filterwarnings('ignore')

common_comid_list = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/Common_stations_hydroweb_and_gauging_stations_v1.csv')

IDs = common_comid_list['ID'].tolist()
COMIDs = common_comid_list['COMID'].tolist()
stations = common_comid_list['Station'].tolist()
Names = common_comid_list['Station_Name'].tolist()
Countries = common_comid_list['Country'].tolist()
Regions = common_comid_list['Region'].tolist()

list = ['max', 'mean', 'min']

for element in list:

    all_station_table = pd.DataFrame()

    for id, comid, name, station, country, region in zip(IDs, COMIDs, Names, stations, Countries, Regions):

        print(element, '-', id, '-', comid, '-', name, '-', station, '-', country, '-', region)

        observed_wl = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Observed_Data_WL/{0}/{1}_{2}_{0}.csv'.format(element, id, station), index_col=0)
        observed_wl.index = pd.to_datetime(observed_wl.index)
        observed_wl = observed_wl.groupby(observed_wl.index.strftime("%Y-%m-%d")).mean()
        observed_wl.index = pd.to_datetime(observed_wl.index)

        corrected_wl = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/Corrected_Data_WL/{0}/{1}-{2}_{3}_{0}.csv'.format(element, id, comid, station), index_col=0)
        corrected_wl.index = pd.to_datetime(corrected_wl.index)
        corrected_wl = corrected_wl.groupby(corrected_wl.index.strftime("%Y-%m-%d")).mean()
        corrected_wl.index = pd.to_datetime(corrected_wl.index)

        # Merging the Data
        merged_df = hd.merge_data(obs_df=observed_wl, sim_df=corrected_wl)

        # Appending the table to the final table
        table = hs.make_table(merged_df, metrics=['R (Pearson)', 'KGE (2012)', 'MAE', 'MAPE', 'ME', 'RMSE', 'NRMSE (Mean)', 'NSE', 'KGE (2009)', 'R (Spearman)', 'r2'], location=name, remove_neg=False, remove_zero=False)
        #all_station_table = all_station_table.append(table)

        '''Tables and Plots'''
        sim_array = merged_df.iloc[:, 0].values
        obs_array = merged_df.iloc[:, 1].values

        sim_mean = statistics.mean(sim_array)
        obs_mean = statistics.mean(obs_array)

        sim_std = statistics.stdev(sim_array)
        obs_std = statistics.stdev(obs_array)

        table['ID'] = id
        table['COMID'] = comid
        table['Satellite'] = station
        table['Bias'] = sim_mean / obs_mean
        table['Variability'] = ((sim_std / sim_mean) / (obs_std / obs_mean))

        all_station_table = all_station_table.append(table)

    print(all_station_table)
    all_station_table.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Hydroweb/data/Common_Places/validationResults/observed_corrected_obs/Metrics_observed_corrected_obs_{0}.csv'.format(element))
