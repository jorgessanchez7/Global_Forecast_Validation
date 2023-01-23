import numpy as np
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

total_stations_Q = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_Q.csv')

sources = total_stations_Q['Data_Source'].tolist()
folders = total_stations_Q['Folder'].tolist()
regions = total_stations_Q['Region'].tolist()
IDs = total_stations_Q['ID'].tolist()
COMIDs = total_stations_Q['COMID'].tolist()
Names = total_stations_Q['Station'].tolist()
latitudes = total_stations_Q['Latitude'].tolist()
longitudes = total_stations_Q['Longitude'].tolist()
areas = total_stations_Q['Area_sim_km2'].tolist()
biass_orig = total_stations_Q['Bias_Original'].tolist()
biass_corr = total_stations_Q['Bias_Corrected'].tolist()
vars_orig = total_stations_Q['KGE_Original'].tolist()
vars_corr = total_stations_Q['Correlation_Original'].tolist()
kges_orig = total_stations_Q['Correlation_Corrected'].tolist()
kges_corr = total_stations_Q['KGE_Corrected'].tolist()

table = []

for source, folder, region, id, comid, name, latitude, longitude, area, bias_orig, bias_corr, var_orig, var_corr, kge_orig, kge_corr in zip(sources, folders, regions, IDs, COMIDs, Names, latitudes, longitudes, areas, biass_orig, biass_corr, vars_orig, vars_corr, kges_orig, kges_corr):

    print(id, ' - ', comid, ' - ', name)

    if source == 'USA':
        if int(id) < 10000000:
            station_id = '0' + str(id)
        else:
            station_id = str(id)
        id = station_id

    observed_data = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Observed_Data/{2}.csv'.format(folder, source,id), index_col=0)
    observed_data[observed_data < 0] = 0
    observed_data.index = pd.to_datetime(observed_data.index)
    observed_data.index = observed_data.index.to_series().dt.strftime("%Y-%m-%d")
    observed_data.index = pd.to_datetime(observed_data.index)

    observed_df = observed_data.dropna()

    date_ini = observed_df.index[0]
    date_end = observed_df.index[len(observed_df.index) - 1]

    total_data = date_end - date_ini
    total_data = int((total_data / np.timedelta64(1, 'D')))

    no_nan_data = len(observed_df.index)
    missing_data = 1 - (no_nan_data/total_data)

    year_ini = date_ini.year
    year_end = date_end.year

    years = np.arange(year_ini, year_end+1, 1)

    total_mean = np.nanmean(observed_data.iloc[:, 0].values)
    total_std = np.nanstd(observed_data.iloc[:, 0].values)

    diff_mean = 0
    diff_std = 0

    for anyo in years:
        yearly_data = observed_data[observed_data.index.year == int(anyo)]
        yearly_mean = np.nanmean(yearly_data.iloc[:, 0].values)
        yearly_std = np.nanstd(yearly_data.iloc[:, 0].values)

        mean_diff = yearly_mean
        year_mean = anyo
        std_diff = yearly_std
        year_std = anyo

        if abs(total_mean-yearly_mean) > diff_mean:
            diff_mean = abs(total_mean-yearly_mean)
            mean_diff = yearly_mean
            year_mean = anyo

        if abs(total_std-yearly_std) > diff_std:
            diff_std = abs(total_std-yearly_std)
            std_diff = yearly_std
            year_std = anyo

    mean_diff_percentage = diff_mean/total_mean
    std_diff_percentage = diff_std/total_std

    table.append([source, folder, region, id, comid, name, latitude, longitude, area, bias_orig, bias_corr, var_orig,
                  var_corr, kge_orig, kge_corr, date_ini, date_end, total_data, no_nan_data, missing_data, total_mean,
                  mean_diff, mean_diff_percentage, year_mean, total_std, std_diff, std_diff_percentage, year_std])

table_df = pd.DataFrame(table, columns=['Data_Source', 'Folder', 'Region', 'ID', 'COMID', 'Station', 'Latitude',
                                        'Longitude', 'Area_sim_km2', 'Bias_Original', 'Bias_Corrected',
                                        'KGE_Original', 'Correlation_Original', 'Correlation_Corrected',
                                        'KGE_Corrected', 'Initial Date', 'Final Date', 'Number Days',
                                        'Total Days Data', '% Missing Data', 'Mean', 'Mean*', '% Mean Difference',
                                        'Year Mean', 'STD', 'STD*', '% STD Difference', 'Year STD'])

table_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_Q_v1.csv')