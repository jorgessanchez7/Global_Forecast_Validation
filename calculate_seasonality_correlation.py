import math
import geoglows
import hydrostats
import statistics
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

    if max_val == min_val:
        warnings.warn('The observational data has the same max and min value. You may get unanticipated results.')
        max_val += .1

    # determine number of histograms bins needed
    number_of_points = len(monthly_data.values)
    number_of_classes = math.ceil(1 + (3.322 * math.log10(number_of_points)))

    # specify the bin width for histogram (in m3/s)
    step_width = (max_val - min_val) / number_of_classes

    # specify histogram bins
    bins = np.arange(-np.min(step_width), max_val + 2 * np.min(step_width), np.min(step_width))

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

stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/World_Stations_v0_v3.csv')
stations_df = stations_df.loc[stations_df['Q'] == 'YES']

codes = stations_df['samplingFeatureCode'].tolist()
names = stations_df['name'].tolist()
ids = stations_df['id'].tolist()
dataSources = stations_df['Data_Source'].tolist()
dataFolders = stations_df['Folder'].tolist()
comids = stations_df['samplingFeatureType'].tolist()

table = []

for code, name, id, dataSource, dataFolder, comid in zip(codes, names, ids, dataSources, dataFolders, comids):
    
    print(code, ' - ', name, ' - ', id, ' - ', dataSource, ' - ', dataFolder, ' - ', comid)
    """
    if dataSource == 'USA':
        if int(code) < 10000000:
            station_id = code
            code = '0' + code
        else:
            code = code
    """
    observed_values = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Observed_Data/{2}.csv'.format(dataFolder, dataSource, code), index_col=0)
    observed_values.index = pd.to_datetime(observed_values.index)
    observed_values.index = observed_values.index.to_series().dt.strftime("%Y-%m-%d")
    observed_values.index = pd.to_datetime(observed_values.index)
    """
    if dataSource == 'USA':
        if int(code) < 10000000:
            observed_values.to_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Observed_Data/{2}.csv'.format(dataFolder, dataSource, station_id))
    """
    column_name_obs = observed_values.columns[0]
    observed_values.rename(columns={column_name_obs: "Observed Streamfow"}, inplace=True)
    day_obs_df = hydrostats.data.daily_average(observed_values, rolling=True)
    mon_obs_df = hydrostats.data.monthly_average(observed_values)

    simulated_values = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Simulated_Data/{2}.csv'.format(dataFolder, dataSource, comid), index_col=0)
    simulated_values.index = pd.to_datetime(simulated_values.index)
    simulated_values.index = simulated_values.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_values.index = pd.to_datetime(simulated_values.index)
    column_name_sim = simulated_values.columns[0]
    simulated_values.rename(columns={column_name_sim: "Simulated Streamfow"}, inplace=True)
    day_sim_df = hydrostats.data.daily_average(simulated_values, rolling=True)
    mon_sim_df = hydrostats.data.monthly_average(simulated_values)

    """
    #if dataSource == 'USA':
    #if dataSource == 'Peru':
    #if code == '747':
    if code == '31097020':
        corrected_values = geoglows.bias.correct_historical(simulated_values, observed_values)
        corrected_values.index = corrected_values.index.to_series().dt.strftime("%Y-%m-%d")
        corrected_values.index = pd.to_datetime(corrected_values.index)
        corrected_values.to_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Corrected_Data/{2}-{3}.csv'.format(dataFolder, dataSource, code, comid))
    """

    corrected_values = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Corrected_Data/{2}-{3}.csv'.format(dataFolder, dataSource, code, comid), index_col=0)
    corrected_values.index = pd.to_datetime(corrected_values.index)
    corrected_values.index = corrected_values.index.to_series().dt.strftime("%Y-%m-%d")
    corrected_values.index = pd.to_datetime(corrected_values.index)

    """
    if dataSource == 'USA':
        if int(code) < 10000000:
            observed_values.to_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/{0}/{1}/data/historical/Corrected_Data/{2}-{3}.csv'.format(dataFolder, dataSource, station_id, station_id))
    """

    column_name_bc = corrected_values.columns[0]
    corrected_values.rename(columns={column_name_bc: "Bias Corrected Streamfow"}, inplace=True)
    day_corr_df = hydrostats.data.daily_average(corrected_values, rolling=True)
    mon_corr_df = hydrostats.data.monthly_average(corrected_values)

    """Seasonal Correlations"""

    daily_merge_obs_sim = pd.concat([day_obs_df, day_sim_df], axis=1)
    daily_merge_obs_sim.dropna(inplace=True)
    daily_seasonal_correlation = hydrostats.metrics.pearson_r(daily_merge_obs_sim.iloc[:, 1].values, daily_merge_obs_sim.iloc[:, 0].values)

    daily_merge_obs_bc = pd.concat([day_obs_df, day_corr_df], axis=1)
    daily_merge_obs_bc.dropna(inplace=True)
    daily_seasonal_correlation_bc = hydrostats.metrics.pearson_r(daily_merge_obs_bc.iloc[:, 1].values, daily_merge_obs_bc.iloc[:, 0].values)

    monthly_merge_obs_sim = pd.concat([mon_obs_df, mon_sim_df], axis=1)
    monthly_merge_obs_sim.dropna(inplace=True)
    monthly_seasonal_correlation = hydrostats.metrics.pearson_r(monthly_merge_obs_sim.iloc[:, 1].values, monthly_merge_obs_sim.iloc[:, 0].values)

    monthly_merge_obs_bc = pd.concat([mon_obs_df, mon_corr_df], axis=1)
    monthly_merge_obs_bc.dropna(inplace=True)
    monthly_seasonal_correlation_bc = hydrostats.metrics.pearson_r(monthly_merge_obs_bc.iloc[:, 1].values, monthly_merge_obs_bc.iloc[:, 0].values)

    #print('Daily Correlation: ', daily_seasonal_correlation, ' - ', daily_seasonal_correlation_bc)
    #print('Monthly Correlation: ', monthly_seasonal_correlation, ' - ', monthly_seasonal_correlation_bc)

    """KGE Calculations"""
    merged_obs_sim = hydrostats.data.merge_data(sim_df=simulated_values, obs_df=observed_values)
    sim_array = merged_obs_sim.iloc[:, 0].values
    obs_array = merged_obs_sim.iloc[:, 1].values
    sim_mean = statistics.mean(sim_array)
    obs_mean = statistics.mean(obs_array)
    sim_std = statistics.stdev(sim_array)
    obs_std = statistics.stdev(obs_array)

    bias_ratio_obs_sim = sim_mean / obs_mean
    variability_ratio_obs_sim = ((sim_std / sim_mean) / (obs_std / obs_mean))
    correlation_ratio_obs_sim = hydrostats.metrics.pearson_r(sim_array, obs_array)
    kge_obs_sim = hydrostats.metrics.kge_2012(sim_array, obs_array)

    bias_percent_obs_sim = abs(bias_ratio_obs_sim - 1)
    variability_percent_obs_sim = abs(variability_ratio_obs_sim - 1)

    merged_obs_bc = hydrostats.data.merge_data(sim_df=corrected_values, obs_df=observed_values)
    bc_array = merged_obs_bc.iloc[:, 0].values
    obs_bc_array = merged_obs_bc.iloc[:, 1].values
    bc_mean = statistics.mean(bc_array)
    obs_bc_mean = statistics.mean(obs_bc_array)
    bc_std = statistics.stdev(bc_array)
    obs_bc_std = statistics.stdev(obs_bc_array)

    bias_ratio_obs_bc = bc_mean / obs_bc_mean
    variability_ratio_obs_bc = ((bc_std / bc_mean) / (obs_bc_std / obs_bc_mean))
    correlation_ratio_obs_bc = hydrostats.metrics.pearson_r(bc_array, obs_bc_array)
    kge_obs_bc = hydrostats.metrics.kge_2012(bc_array, obs_bc_array)

    bias_percent_obs_bc = abs(bias_ratio_obs_bc - 1)
    variability_percent_obs_bc = abs(variability_ratio_obs_bc - 1)

    """Statistics and Percentile Calculation"""
    obs_mean = np.nanmean(observed_values.iloc[:, 0].values)
    obs_std = np.nanstd(observed_values.iloc[:, 0].values)
    obs_cv = obs_std / obs_mean

    obs_min = np.nanmin(observed_values.iloc[:, 0].values)
    obs_p10 = np.nanpercentile(observed_values.iloc[:, 0].values, 10)
    obs_p20 = np.nanpercentile(observed_values.iloc[:, 0].values, 20)
    obs_p25 = np.nanpercentile(observed_values.iloc[:, 0].values, 25)
    obs_p30 = np.nanpercentile(observed_values.iloc[:, 0].values, 30)
    obs_p40 = np.nanpercentile(observed_values.iloc[:, 0].values, 40)
    obs_median = np.nanpercentile(observed_values.iloc[:, 0].values, 50)
    obs_p60 = np.nanpercentile(observed_values.iloc[:, 0].values, 60)
    obs_p70 = np.nanpercentile(observed_values.iloc[:, 0].values, 70)
    obs_p75 = np.nanpercentile(observed_values.iloc[:, 0].values, 75)
    obs_p80 = np.nanpercentile(observed_values.iloc[:, 0].values, 80)
    obs_p90 = np.nanpercentile(observed_values.iloc[:, 0].values, 90)
    obs_max = np.nanmax(observed_values.iloc[:, 0].values)

    obs_min_ratio = obs_min / obs_mean
    obs_p10_ratio = obs_p10 / obs_mean
    obs_p20_ratio = obs_p20 / obs_mean
    obs_p25_ratio = obs_p25 / obs_mean
    obs_p30_ratio = obs_p30 / obs_mean
    obs_p40_ratio = obs_p40 / obs_mean
    obs_median_ratio = obs_median / obs_mean
    obs_p60_ratio = obs_p60 / obs_mean
    obs_p70_ratio = obs_p70 / obs_mean
    obs_p75_ratio = obs_p75 / obs_mean
    obs_p80_ratio = obs_p80 / obs_mean
    obs_p90_ratio = obs_p90 / obs_mean
    obs_max_ratio = obs_max / obs_mean
    
    """Statistics After 1979-01-01"""
    observed_values_1979 = observed_values.loc[observed_values.index >= pd.to_datetime("1979-01-01")]

    obs_mean_1979 = np.nanmean(observed_values_1979.iloc[:, 0].values)
    obs_std_1979 = np.nanstd(observed_values_1979.iloc[:, 0].values)
    obs_cv_1979 = obs_std_1979 / obs_mean_1979
    obs_max_1979 = np.nanmax(observed_values_1979.iloc[:, 0].values)

    mean_ratio = obs_mean_1979 / obs_mean
    std_ratio = obs_std_1979 / obs_std
    cv_ratio = obs_cv_1979 / obs_cv
    max_ratio = obs_max_1979 / obs_max

    """Flow Duration Curve Correlations"""
    observed_values.dropna(inplace=True)

    total_sim_data = len(simulated_values.index)
    total_obs_data = len(observed_values.index)
    total_data = max(total_obs_data, total_sim_data)

    to_flow_sim = _flow_and_probability_mapper(simulated_values, to_flow=True)
    to_flow_obs = _flow_and_probability_mapper(observed_values, to_flow=True, extrapolate=True)

    x_sim = np.arange(0, 1, (1 / total_data))
    x_sim = np.around(x_sim, decimals=10)
    y_sim = to_flow_sim(x_sim)  # use interpolation function returned by `interp1d`
    y_sim = np.around(y_sim, decimals=10)

    x_obs = np.arange(0, 1, (1 / total_data))
    x_obs = np.around(x_obs, decimals=10)
    y_obs = to_flow_obs(x_obs)  # use interpolation function returned by `interp1d`
    y_obs = np.around(y_obs, decimals=10)

    mean_sim = average(simulated_values.mean())
    mean_obs = average(observed_values.mean())

    y_sim_adj = y_sim / mean_sim

    if mean_obs == 0:
        y_obs_adj = y_obs
    else:
        y_obs_adj = y_obs / mean_obs

    r_pearson_total = he.pearson_r(y_sim, y_obs)

    # monthly fdc
    unique_simulation_months = sorted(set(simulated_values.index.strftime('%m')))

    r_pearson = []

    for mes in unique_simulation_months:
        monthly_simulated = simulated_values[simulated_values.index.month == int(mes)].dropna()
        to_flow_sim = _flow_and_probability_mapper(monthly_simulated, to_flow=True)

        total_sim_data = len(monthly_simulated.index)

        monthly_observed = observed_values[observed_values.index.month == int(mes)].dropna()
        to_flow_obs = _flow_and_probability_mapper(monthly_observed, to_flow=True)

        total_obs_data = len(monthly_observed.index)
        total_data = max(total_sim_data, total_obs_data)

        x_sim = np.arange(0, 1, (1 / total_data))
        x_sim = np.around(x_sim, decimals=10)
        y_sim = to_flow_sim(x_sim)  # use interpolation function returned by `interp1d`
        y_sim = np.around(y_sim, decimals=10)

        x_obs = np.arange(0, 1, (1 / total_data))
        x_obs = np.around(x_obs, decimals=10)
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

        r_pearson.append(he.pearson_r(y_sim, y_obs))

    r_pearson_def = np.min(r_pearson)
    r_pearson_mean = np.mean(r_pearson)

    table.append([dataFolder, dataSource, id, code, name, comid, daily_seasonal_correlation,
                  daily_seasonal_correlation_bc, monthly_seasonal_correlation, monthly_seasonal_correlation_bc,
                  bias_ratio_obs_sim, variability_ratio_obs_sim, correlation_ratio_obs_sim, kge_obs_sim, bias_percent_obs_sim,
                  variability_percent_obs_sim, bias_ratio_obs_bc, variability_ratio_obs_bc, correlation_ratio_obs_bc,
                  kge_obs_bc, bias_percent_obs_bc, variability_percent_obs_bc, obs_mean, obs_std, obs_cv, obs_min, obs_p10,
                  obs_p20, obs_p25, obs_p30, obs_p40, obs_median, obs_p60, obs_p70, obs_p75, obs_p80, obs_p90 , obs_max,
                  obs_min_ratio, obs_p10_ratio, obs_p20_ratio, obs_p25_ratio, obs_p30_ratio, obs_p40_ratio, obs_median_ratio,
                  obs_p60_ratio, obs_p70_ratio, obs_p75_ratio, obs_p80_ratio, obs_p90_ratio, obs_max_ratio, obs_mean_1979,
                  obs_std_1979, obs_cv_1979, obs_max_1979, mean_ratio, std_ratio, cv_ratio, max_ratio, r_pearson_total,
                  r_pearson_def, r_pearson_mean])
    
table_df = pd.DataFrame(table, columns=['Data Folder', 'Data Source', 'id', 'code', 'Name', 'COMID', 'r_day',
                                        'r_day_bc', 'r_month', 'r_month_bc', 'Bias_Original', 'Variability_Original',
                                        'Correlation_Original', 'KGE_Original', 'Bias Original (%)', 'Variability Original (%)',
                                        'Bias_Corrected', 'Variability_Corrected', 'Correlation_Corrected', 'KGE_Corrected',
                                        'Bias Corrected (%)', 'Variability Corrected (%)', 'Mean', 'STD', 'CV', 'Min',
                                        'Percentile_10', 'Percentile_20', 'Percentile_25', 'Percentile_30', 'Percentile_40',
                                        'Median', 'Percentile_60', 'Percentile_70', 'Percentile_75',
                                        'Percentile_80', 'Percentile_90', 'Max', 'Min/Mean', 'Percentile_10/Mean',
                                        'Percentile_20/Mean', 'Percentile_25/Mean', 'Percentile_30/Mean', 'Percentile_40/Mean',
                                        'Median/Mean', 'Percentile_60/Mean', 'Percentile_70/Mean', 'Percentile_75/Mean',
                                        'Percentile_80/Mean', 'Percentile_90/Mean', 'Max/Mean', 'Mean After 1979',
                                        'STD After 1979', 'CV After 1979', 'Max After 1979', 'Mean1979/Mean', 'STD1979/STD',
                                        'CV1979/CV', 'Max1979/Max', 'Correlation_FDC_Total_Data', 'Correlation_FDC_Month_Data_min',
                                        'Correlation_FDC_Month_Data_mean'])

#table_df.to_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/USA_Total_Correlations.csv')
table_df.set_index('id', inplace=True)
table_df.to_csv('/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/World_Total_Correlations.csv')