import math
import numpy as np
import pandas as pd
import HydroErr as he
from scipy import interpolate
from scipy.stats import ks_2samp
from numpy.ma.extras import average

import warnings
warnings.filterwarnings("ignore")

def _flow_and_probability_mapper(monthly_data: pd.DataFrame, to_probability: bool = False, to_flow: bool = False,
                                 extrapolate: bool = False) -> interpolate.interp1d:
    if not to_flow and not to_probability:
        raise ValueError('You need to specify either to_probability or to_flow as True')

    # get maximum value to bound histogram
    max_val = math.ceil(np.max(monthly_data.max()))
    min_val = math.floor(np.min(monthly_data.min()))

    #print(max_val)
    #print(min_val)

    if max_val == min_val:
        warnings.warn('The observational data has the same max and min value. You may get unanticipated results.')
        max_val += .1

    # determine number of histograms bins needed
    number_of_points = len(monthly_data.values)
    number_of_classes = math.ceil(1 + (3.322 * math.log10(number_of_points)))

    # specify the bin width for histogram (in m3/s)
    step_width = (max_val - min_val) / number_of_classes

    # specify histogram bins
    if min_val >= 0:
        bins = np.arange(-np.min(step_width), max_val + 2 * np.min(step_width), np.min(step_width))
    else:
        bins = np.arange(min_val - np.min(step_width), max_val + 2 * np.min(step_width), np.min(step_width))
    #print (bins)

    if bins[0] == 0:
        bins = np.concatenate((-bins[1], bins))
    elif bins[0] > 0:
        bins = np.concatenate((-bins[0], bins))

    # make the histogram
    counts, bin_edges = np.histogram(monthly_data, bins=bins)

    # adjust the bins to be the center
    bin_edges = bin_edges[1:]
    # normalize the histograms
    counts = counts.astype(float) / monthly_data.size
    # calculate the cdfs
    cdf = np.cumsum(counts)
    cdf = np.around(cdf, decimals=10)
    #print(cdf)
    # interpolated function to convert simulated streamflow to prob
    if to_probability:
        if extrapolate:
            return interpolate.interp1d(bin_edges, cdf, fill_value='extrapolate')
        return interpolate.interp1d(bin_edges, cdf)
    # interpolated function to convert simulated prob to observed streamflow
    elif to_flow:
        if extrapolate:
            return interpolate.interp1d(cdf, bin_edges, fill_value='extrapolate')
        return interpolate.interp1d(cdf, bin_edges)

#total_stations_Q = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL.csv')
#total_stations_Q = pd.read_csv('/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL.csv')
#total_stations_Q = pd.read_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jlsanchezlo@unal.edu.co/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL.csv")
total_stations_Q = pd.read_csv("/Volumes/Macintosh HD/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL.csv")

sources = total_stations_Q['Data_Source'].tolist()
folders = total_stations_Q['Folder'].tolist()
regions = total_stations_Q['Region'].tolist()
countries = total_stations_Q['Country'].tolist()
IDs = total_stations_Q['ID'].tolist()
COMIDs = total_stations_Q['COMID'].tolist()
Names = total_stations_Q['Station'].tolist()
latitudes = total_stations_Q['Latitude'].tolist()
longitudes = total_stations_Q['Longitude'].tolist()
areas = total_stations_Q['Area_sim_km2'].tolist()
biass_corr = total_stations_Q['Bias'].tolist()
vars_corr = total_stations_Q['Variability'].tolist()
corrs_corr = total_stations_Q['Correlation'].tolist()
kges_corr = total_stations_Q['KGE'].tolist()

table = []

for source, folder, region, country, id, comid, name, latitude, longitude, area, bias_corr, var_corr, corr_corr, kge_corr in zip(sources, folders, regions, countries, IDs, COMIDs, Names, latitudes, longitudes, areas, biass_corr, vars_corr, corrs_corr, kges_corr):

    print(id, ' - ', comid, ' - ', name)

    if source == 'USA':
        if int(id) < 10000000:
            station_id = '0' + str(id)
        else:
            station_id = str(id)
        id = station_id

    #observed_data = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Observed_Data_WL/{2}.csv'.format(folder, source,id), index_col=0)
    observed_data = pd.read_csv('/Volumes/Macintosh HD/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Observed_Data_WL/{2}.csv'.format(folder, source, id), index_col=0)
    observed_data.index = pd.to_datetime(observed_data.index)
    observed_data.index = observed_data.index.to_series().dt.strftime("%Y-%m-%d")
    observed_data.index = pd.to_datetime(observed_data.index)

    observed_df = observed_data.dropna()

    date_ini = observed_df.index[0]
    date_end = observed_df.index[len(observed_df.index) - 1]

    total_data_obs = date_end - date_ini
    total_data_obs = int((total_data_obs / np.timedelta64(1, 'D')))

    no_nan_data = len(observed_df.index)
    missing_data = 1 - (no_nan_data/total_data_obs)

    recent_observed_data = observed_data.loc[observed_data.index >= pd.to_datetime('1980-01-01')]
    recent_observed_data.dropna(inplace=True)
    no_nan_recent_data = len(recent_observed_data.index)

    year_ini = date_ini.year
    year_end = date_end.year

    years = np.arange(year_ini, year_end+1, 1)

    total_mean = np.nanmean(observed_data.iloc[:, 0].values)
    total_std = np.nanstd(observed_data.iloc[:, 0].values)

    total_median = np.nanpercentile(observed_data.iloc[:, 0].values, 50)

    total_p25 = np.nanpercentile(observed_data.iloc[:, 0].values, 25)
    total_p75 = np.nanpercentile(observed_data.iloc[:, 0].values, 75)

    total_min = np.nanmin(observed_data.iloc[:, 0].values)
    total_max = np.nanmax(observed_data.iloc[:, 0].values)

    ##Calculating error metrics on fdc
    simulated_data = pd.read_csv('/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Simulated_Data_WL/{2}.csv'.format(folder, source, comid), index_col=0)
    simulated_data[simulated_data < 0] = 0
    simulated_data.index = pd.to_datetime(simulated_data.index)
    simulated_data.index = simulated_data.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_data.index = pd.to_datetime(simulated_data.index)
    simulated_data = simulated_data.loc[simulated_data.index >= pd.to_datetime('1980-01-01')]

    min_value = np.min(observed_data.min())

    if min_value >= 0:
        min_value = 0

    observed_data = observed_data - min_value

    observed_data.dropna(inplace=True)

    total_sim_data = len(simulated_data.index)
    total_obs_data = len(observed_data.index)
    total_data = max(total_obs_data, total_sim_data)

    to_flow_sim = _flow_and_probability_mapper(simulated_data, to_flow=True)
    to_flow_obs = _flow_and_probability_mapper(observed_data, to_flow=True, extrapolate=True)

    x_sim = np.arange(0, 1, (1 / total_data))
    x_sim = np.around(x_sim, decimals=10)
    y_sim = to_flow_sim(x_sim)  # use interpolation function returned by `interp1d`
    y_sim = np.around(y_sim, decimals=10)

    x_obs = np.arange(0, 1, (1 / total_data))
    x_obs = np.around(x_obs, decimals=10)
    y_obs = to_flow_obs(x_obs)  # use interpolation function returned by `interp1d`
    y_obs = np.around(y_obs, decimals=10)

    mean_sim = average(simulated_data.mean())
    mean_obs = average(observed_data.mean())

    y_sim_adj = y_sim / mean_sim

    if mean_obs == 0:
        y_obs_adj = y_obs
    else:
        y_obs_adj = y_obs / mean_obs

    kge_total = he.kge_2012(y_sim_adj, y_obs_adj)
    nse_total = he.nse(y_sim_adj, y_obs_adj)
    ks_total, p_value = ks_2samp(y_obs, y_sim)

    # monthly fdc
    unique_simulation_months = sorted(set(simulated_data.index.strftime('%m')))

    kge = []
    nse = []
    ks = []

    for mes in unique_simulation_months:

        monthly_simulated = simulated_data[simulated_data.index.month == int(mes)].dropna()
        to_flow_sim = _flow_and_probability_mapper(monthly_simulated, to_flow=True)

        total_sim_data = len(monthly_simulated.index)

        monthly_observed = observed_data[observed_data.index.month == int(mes)].dropna()
        to_flow_obs = _flow_and_probability_mapper(monthly_observed, to_flow=True)

        total_sim_data = len(monthly_observed.index)
        total_data = max(total_sim_data, total_sim_data)

        x_sim = np.arange(0, 1, (1 / total_data))
        x_sim = np.around(x_sim, decimals=10)
        y_sim = to_flow_sim(x_sim)  # use interpolation function returned by `interp1d`
        y_sim = np.around(y_sim, decimals=10)

        x_obs = np.arange(0, 1, (1 / total_data))
        x_obs = np.around(x_obs, decimals=10)
        #for xs in x_obs:
        #    print(xs)
        #    print(to_flow_obs(xs))
        y_obs = to_flow_obs(x_obs)  # use interpolation function returned by `interp1d`
        y_obs = np.around(y_obs, decimals=10)

        ks_statistic, p_value = ks_2samp(y_obs, y_sim)

        mean_sim = average(monthly_simulated.mean())
        mean_obs = average(monthly_observed.mean())

        y_sim_adj = y_sim / mean_sim

        if mean_obs == 0:
            y_obs_adj = y_obs
        else:
            y_obs_adj = y_obs / mean_obs

        kge.append(he.kge_2012(y_sim_adj, y_obs_adj))
        nse.append(he.nse(y_sim_adj, y_obs_adj))
        ks.append(ks_statistic)

    kge_def = np.min(kge)
    nse_def = np.min(nse)
    ks_def = np.max(ks)

    kge_mean = np.mean(kge)
    nse_mean = np.mean(nse)
    ks_mean = np.mean(ks)

    table.append([source, folder, region, country, id, comid, name, latitude, longitude, area, bias_corr, corr_corr, var_corr,
                  kge_corr, date_ini, date_end, total_data_obs, no_nan_data, no_nan_recent_data, missing_data, total_mean,
                  total_std, total_median, total_p25, total_p75, total_min, total_max, kge_total, nse_total, ks_total,
                  kge_def, nse_def, ks_def, kge_mean, nse_mean, ks_mean])

table_df = pd.DataFrame(table, columns=['Data_Source', 'Folder', 'Region', 'Country', 'ID', 'COMID', 'Station', 'Latitude',
                                        'Longitude', 'Area_sim_km2', 'Bias', 'Variability', 'Correlation', 'KGE',
                                        'Initial Date', 'Final Date', 'Number Days', 'Total Days Data', 'Total Days Recent Data',
                                        '% Missing Data', 'Mean', 'STD', 'Median', 'Percentile 25', 'Percentile 75', 'Min',
                                        'Max', 'KGE_total_FDC', 'NSE_total_FDC', 'Kolmogorov-Smirnov_total_FDC',
                                        'KGE_month_FDC', 'NSE_month_FDC', 'Kolmogorov-Smirnov_month_FDC', 'KGE_month_mean_FDC',
                                        'NSE_month_mean_FDC', 'Kolmogorov-Smirnov_month_mean_FDC'])

#table_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL_v1.csv')
#table_df.to_csv('/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13/World_Total_Stations_WL_v2.csv')
#table_df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jlsanchezlo@unal.edu.co/My Drive/PhD/2022_Winter/Dissertation_v13//World_Total_Stations_WL_v2.csv")
table_df.to_csv("/Volumes/Macintosh HD/Users/grad/Google Drive/My Drive/PhD/2022_Winter/Dissertation_v13//World_Total_Stations_WL_v2.csv")