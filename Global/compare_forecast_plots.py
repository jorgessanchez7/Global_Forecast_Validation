import io
import os
import math
import math
import requests
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings('ignore')

def get_forecast_records_v1(river_id, start_date):
    """
    Fetches forecast data from a given start date to today, calculates the mean of the first 8 time steps,
    and appends results for consecutive forecast dates.

    Args:
        get_forecast_records (function): A function that retrieves forecast records for a given date and river ID.
        river_id (int): The ID of the river for which forecast data will be retrieved.
        start_date (str): The starting date in YYYYMMDD format.
    Returns:
        pd.DataFrame: A DataFrame with the forecast records.
    """
    # Initialize an empty DataFrame to hold mean values
    forecast_records_df = pd.DataFrame()

    # Convert start date to a datetime object
    start_date = dt.datetime.strptime(start_date, '%Y%m%d')

    # Iterate over each day from the start date to today
    current_date = start_date
    end_date = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    while current_date <= end_date:

        yyyy = str(current_date.year)
        mm = current_date.month

        if mm < 10:
            mm ='0{0}'.format(mm)
        else:
            mm = str(mm)
        dd = current_date.day

        if dd < 10:
            dd ='0{0}'.format(dd)
        else:
            dd = str(dd)

        # Fetch forecast data for the current date
        print(f'{yyyy}-{mm}-{dd} v1')

        try:
            url_1 = 'https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={0}&date={1}{2}{3}&ensemble=1-51&return_format=csv'.format(river_id, yyyy, mm, dd)
            s = requests.get(url_1, verify=False).content
            forecast_data = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
            #forecast_data = geoglows.data.forecast_ensembles(river_id, date='{0}{1}{2}'.format(yyyy,mm,dd))
            forecast_data.index = pd.to_datetime(forecast_data.index)
            forecast_data[forecast_data < 0] = 0
            forecast_data.index = forecast_data.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            forecast_data.index = pd.to_datetime(forecast_data.index)
            #forecast_data = forecast_data.drop(['ensemble_52'], axis=1)
            forecast_data.dropna(inplace=True)

            mean_df = forecast_data.mean(axis=1).to_frame()
            mean_df.rename(columns = {0:'average_flow'}, inplace = True)

            if isinstance(mean_df, pd.DataFrame):
                # Extract the first 8 time steps
                summary_df = mean_df.iloc[:8]
                # Append to the main DataFrame
                forecast_records_df = pd.concat([forecast_records_df, summary_df])

        except Exception as e:
            print(e)

        # Move to the next day
        current_date += dt.timedelta(days=1)

    return forecast_records_df

def get_forecast_records_v2(river_id, start_date):
    """
    Fetches forecast data from a given start date to today, calculates the mean of the first 8 time steps,
    and appends results for consecutive forecast dates.

    Args:
        get_forecast_records (function): A function that retrieves forecast records for a given date and river ID.
        river_id (int): The ID of the river for which forecast data will be retrieved.
        start_date (str): The starting date in YYYYMMDD format.
    Returns:
        pd.DataFrame: A DataFrame with the forecast records.
    """
    # Initialize an empty DataFrame to hold mean values
    forecast_records_df = pd.DataFrame()

    # Convert start date to a datetime object
    start_date = dt.datetime.strptime(start_date, '%Y%m%d')

    # Iterate over each day from the start date to today
    current_date = start_date
    end_date = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    while current_date <= end_date:

        yyyy = str(current_date.year)
        mm = current_date.month

        if mm < 10:
            mm ='0{0}'.format(mm)
        else:
            mm = str(mm)
        dd = current_date.day

        if dd < 10:
            dd ='0{0}'.format(dd)
        else:
            dd = str(dd)

        # Fetch forecast data for the current date
        print(f'{yyyy}-{mm}-{dd} v2')

        try:
            url_1 = 'https://geoglows.ecmwf.int/api/v2/forecastensemble/{0}?date={1}{2}{3}&format=csv'.format(river_id, yyyy,mm,dd)
            s = requests.get(url_1, verify=False).content
            forecast_data = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
            forecast_data.index = pd.to_datetime(forecast_data.index)
            forecast_data[forecast_data < 0] = 0
            forecast_data.index = forecast_data.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            forecast_data.index = pd.to_datetime(forecast_data.index)
            forecast_data = forecast_data.drop(['ensemble_52'], axis=1)
            forecast_data.dropna(inplace=True)

            mean_df = forecast_data.mean(axis=1).to_frame()
            mean_df.rename(columns = {0:'average_flow'}, inplace = True)

            if isinstance(mean_df, pd.DataFrame):
                # Extract the first 8 time steps
                summary_df = mean_df.iloc[:8]
                # Append to the main DataFrame
                forecast_records_df = pd.concat([forecast_records_df, summary_df])

        except Exception as e:
            print(e)

        # Move to the next day
        current_date += dt.timedelta(days=1)

    return forecast_records_df

def gumbel_1(std: float, xbar: float, rp: int or float) -> float:
  """
  Solves the Gumbel Type I probability distribution function (pdf) = exp(-exp(-b)) where b is the covariate. Provide
  the standard deviation and mean of the list of annual maximum flows. Compare scipy.stats.gumbel_r
  Args:
    std (float): the standard deviation of the series
    xbar (float): the mean of the series
    rp (int or float): the return period in years
  Returns:
    float, the flow corresponding to the return period specified
  """
  # xbar = statistics.mean(year_max_flow_list)
  # std = statistics.stdev(year_max_flow_list, xbar=xbar)
  return -math.log(-math.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std)

def return_period_values(time_series_df, comid):

    max_annual_flow = time_series_df.groupby(time_series_df.index.year).max()
    mean_value = np.mean(max_annual_flow.values.flatten())
    std_value = np.std(max_annual_flow.values.flatten())

    return_periods = [2, 5, 10, 25, 50, 100]

    return_periods_values = [gumbel_1(std_value, mean_value, rp) for rp in return_periods]

    d = {'rivid': [comid],
         2: [return_periods_values[0]],
         5: [return_periods_values[1]],
         10: [return_periods_values[2]],
         25: [return_periods_values[3]],
         50: [return_periods_values[4]],
         100: [return_periods_values[5]]}

    rperiods_df = pd.DataFrame(data=d)
    rperiods_df.set_index('rivid', inplace=True)

    rperiods_df = rperiods_df.T

    rperiods_df.columns.name = 'river_id'
    rperiods_df.index.name = 'return_period'

    return rperiods_df

def forecast_stats_records_matplotlib(ax, df: pd.DataFrame, *, rp_df: pd.DataFrame = None, records_df: pd.DataFrame = None, plot_titles: list = None, show_maxmin: bool = False):
    """
    Makes the streamflow data and optional metadata into a matplotlib plot

    Args:
        df: the csv response from forecast_stats
        rp_df: the csv response from return_periods
        records_df: dataframe containing observed or historical data to be added to the plot
        plot_titles: a list of strings to place in the figure title. each list item will be on a new line.
        show_maxmin: Choose to show or hide the max/min envelope by default
    """

    # Create separate dates arrays
    dates_stats = df['flow_avg'].dropna().index.tolist()  # Common dates for statistics
    dates_high_res = df['high_res'].dropna().index.tolist()  # Separate dates for high-resolution data

    # Prepare the plot data, slicing to the min_length
    plot_data = {
        'flow_max': df['flow_max'].dropna(),
        'flow_75%': df['flow_75p'].dropna(),
        'flow_avg': df['flow_avg'].dropna(),
        'flow_med': df['flow_med'].dropna(),
        'flow_25%': df['flow_25p'].dropna(),
        'flow_min': df['flow_min'].dropna(),
        'high_res': df['high_res'].dropna(),
    }

    # Create the figure
    #fig, ax = plt.subplots(figsize=(10, 6))

    # Plot max/min envelope if required
    # if show_maxmin:
    #    ax.fill_between(dates_stats, plot_data['flow_max'], plot_data['flow_min'], color='lightblue', label='Maximum & Minimum Flow')

    ax.fill_between(dates_stats, plot_data['flow_max'], plot_data['flow_min'], color='lightblue',label='Maximum & Minimum Flow', edgecolor='black', linestyle='--')

    # Plot percentiles
    ax.fill_between(dates_stats, plot_data['flow_75%'], plot_data['flow_25%'], color='lightgreen',label='25-75 Percentile Flow', edgecolor='green')

    # Plot other data
    ax.plot(dates_stats, plot_data['flow_avg'], label='Average Flow', color='blue')
    ax.plot(dates_stats, plot_data['flow_med'], label='Median Flow', color='red')
    ax.plot(dates_high_res, plot_data['high_res'], label='High-Resolution Forecast', color='black')

    # Plot records_df if provided
    if records_df is not None and len(records_df) > 0:
        ax.plot(records_df.index, records_df.iloc[:, 0].values, label='1st day Forecast', color='#FFA15A')

    if rp_df is not None and len(rp_df) > 0:
        y_max = max(df['flow_max'])
        r2 = int(rp_df[2].values[0])
        r5 = int(rp_df[5].values[0])
        r10 = int(rp_df[10].values[0])
        r25 = int(rp_df[25].values[0])
        r50 = int(rp_df[50].values[0])
        r100 = int(rp_df[100].values[0])
        rmax = int(max(r100 + (r100*0.05), y_max))

        if records_df is not None and len(records_df) > 0:
            ax.fill_between([records_df.index[0], dates_stats[-1]], [r100 * 0.05, r100 * 0.05], [r100 * 0.05, r100 * 0.05], color=(0 / 255, 0 / 255, 0 / 255, 0), label='Return Periods')
            ax.fill_between([records_df.index[0], dates_stats[-1]], [r2, r2], [r5, r5], color=(254 / 255, 240 / 255, 1 / 255, 0.4), label='2 Year: {}'.format(r2), edgecolor=(254 / 255, 240 / 255, 1 / 255))
            ax.fill_between([records_df.index[0], dates_stats[-1]], [r5, r5], [r10, r10], color=(253 / 255, 154 / 255, 1 / 255, 0.4), label='5 Year: {}'.format(r5), edgecolor=(253 / 255, 154 / 255, 1 / 255))
            ax.fill_between([records_df.index[0], dates_stats[-1]], [r10, r10], [r25, r25], color=(255 / 255, 56 / 255, 5 / 255, 0.4), label='10 Year: {}'.format(r10), edgecolor=(255 / 255, 56 / 255, 5 / 255))
            ax.fill_between([records_df.index[0], dates_stats[-1]], [r25, r25], [r50, r50], color=(255 / 255, 0 / 255, 0 / 255, 0.4), label='25 Year: {}'.format(r25), edgecolor=(255 / 255, 0 / 255, 0 / 255))
            ax.fill_between([records_df.index[0], dates_stats[-1]], [r50, r50], [r100, r100], color=(128 / 255, 0 / 255, 106 / 255, 0.4), label='50 Year: {}'.format(r50), edgecolor=(128 / 255, 0 / 255, 106 / 255))
            ax.fill_between([records_df.index[0], dates_stats[-1]], [r100, r100], [rmax, rmax], color=(128 / 255, 0 / 255, 246 / 255, 0.4), label='100 Year: {}'.format(r100), edgecolor=(128 / 255, 0 / 255, 246 / 255))
        else:
            ax.fill_between([dates_stats[0], dates_stats[-1]], [r100 * 0.05, r100 * 0.05], [r100 * 0.05, r100 * 0.05], color=(0 / 255, 0 / 255, 0 / 255, 0), label='Return Periods')
            ax.fill_between([dates_stats[0], dates_stats[-1]], [r2, r2], [r5, r5], color=(254 / 255, 240 / 255, 1 / 255, 0.4), label='2 Year: {}'.format(r2), edgecolor=(254 / 255, 240 / 255, 1 / 255))
            ax.fill_between([dates_stats[0], dates_stats[-1]], [r5, r5], [r10, r10], color=(253 / 255, 154 / 255, 1 / 255, 0.4), label='5 Year: {}'.format(r5), edgecolor=(253 / 255, 154 / 255, 1 / 255))
            ax.fill_between([dates_stats[0], dates_stats[-1]], [r10, r10], [r25, r25], color=(255 / 255, 56 / 255, 5 / 255, 0.4), label='10 Year: {}'.format(r10), edgecolor=(255 / 255, 56 / 255, 5 / 255))
            ax.fill_between([dates_stats[0], dates_stats[-1]], [r25, r25], [r50, r50], color=(255 / 255, 0 / 255, 0 / 255, 0.4), label='25 Year: {}'.format(r25), edgecolor=(255 / 255, 0 / 255, 0 / 255))
            ax.fill_between([dates_stats[0], dates_stats[-1]], [r50, r50], [r100, r100], color=(128 / 255, 0 / 255, 106 / 255, 0.4), label='50 Year: {}'.format(r50), edgecolor=(128 / 255, 0 / 255, 106 / 255))
            ax.fill_between([dates_stats[0], dates_stats[-1]], [r100, r100], [rmax, rmax], color=(128 / 255, 0 / 255, 246 / 255, 0.4), label='100 Year: {}'.format(r100), edgecolor=(128 / 255, 0 / 255, 246 / 255))

    # Dynamically setting x-ticks
    if records_df is not None and len(records_df) > 0:
        ax.set_xlim(records_df.index[0], dates_stats[-1])
        ax.set_xticks(pd.date_range(start=records_df.index[0], end=dates_stats[-1], periods=6))
    else:
        # Only forecast dates
        ax.set_xlim(dates_stats[0], dates_stats[-1])
        ax.set_xticks(pd.date_range(start=dates_stats[0], end=dates_stats[-1], periods=5))

    # Set title and labels
    ax.set_title('\n'.join(plot_titles) if plot_titles else 'Forecasted Streamflow', fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Streamflow (m³/s)')

    # Rotate date labels for better readability
    plt.xticks(rotation=0)

    # Add grid
    ax.grid(True, linestyle='--', alpha=0.7)  # Add grid with dashed lines and transparency

    # Create the legend and set the font weight to bold
    legend = ax.legend(loc='upper left', fontsize=7)
    for text in legend.get_texts():
        if text.get_text() == 'Return Periods':
            text.set_fontweight('bold')
        if rp_df is not None and len(rp_df) > 0:
            if '2 Year: {}'.format(r2) in text.get_text():
                text.set_fontsize(5)  # Set font size for '2 Year'
            if '5 Year: {}'.format(r5) in text.get_text():
                text.set_fontsize(5)  # Set font size for '5 Year'
            if '10 Year: {}'.format(r10) in text.get_text():
                text.set_fontsize(5)  # Set font size for '10 Year'
            if '25 Year: {}'.format(r25) in text.get_text():
                text.set_fontsize(5)  # Set font size for '25 Year'
            if '50 Year: {}'.format(r50) in text.get_text():
                text.set_fontsize(5)  # Set font size for '50 Year'
            if '100 Year: {}'.format(r100) in text.get_text():
                text.set_fontsize(5)  # Set font size for '100 Year'

def forecast_stats(ensembles_df):
    ensemble = ensembles_df.copy()
    high_res_df = ensemble['ensemble_52'].to_frame()
    ensemble.drop(columns=['ensemble_52'], inplace=True)
    ensemble.dropna(inplace=True)
    high_res_df.dropna(inplace=True)

    max_df = ensemble.quantile(1.0, axis=1).to_frame()
    max_df.rename(columns={1.0: 'flow_max'}, inplace=True)

    p75_df = ensemble.quantile(0.75, axis=1).to_frame()
    p75_df.rename(columns={0.75: 'flow_75p'}, inplace=True)

    p50_df = ensemble.quantile(0.50, axis=1).to_frame()
    p50_df.rename(columns={0.50: 'flow_med'}, inplace=True)

    p25_df = ensemble.quantile(0.25, axis=1).to_frame()
    p25_df.rename(columns={0.25: 'flow_25p'}, inplace=True)

    min_df = ensemble.quantile(0, axis=1).to_frame()
    min_df.rename(columns={0.0: 'flow_min'}, inplace=True)

    mean_df = ensemble.mean(axis=1).to_frame()
    mean_df.rename(columns={0: 'flow_avg'}, inplace=True)

    high_res_df.rename(columns={'ensemble_52': 'high_res'}, inplace=True)

    forecast_stats_df = pd.concat([max_df, p75_df, mean_df, p50_df, p25_df, min_df, high_res_df], axis=1)

    return forecast_stats_df

#Station 0
#comid_1 = 9015333
#comid_2 = 610353609
#name = 'Guaviare River at Mapiripan (Colombia)'

#Station 0Ec
#comid_1 = 9021481
#comid_2 = 670086330
#name = 'Esmeraldas River at Esmeraldas (Ecuador)'

#Station 0Ec
#comid_1 = 9031188
#comid_2 = 670081320
#name = 'Guayas River at Duran (Ecuador)'

#Station 0Ec
comid_1 = 9025707
comid_2 = 670089457
name = 'Chone River after joint with Tosagua River (Ecuador)'

#Station 1
#comid_1 = 9017966
#comid_2 = 620569841
#name = 'Amazonas_casiquiare_km2764'

#Station 2
#comid_1 = 9075530
#comid_2 = 620953148
#name = 'Amazonas_guapore_km3139'

#Station 4
#comid_1 = 9020865
#comid_2 = 620803818
#name = 'Amazonas_negro_km2523'

#Station 5
#comid_1 = 9039185
#comid_2 = 621073787
#name = 'Amazonas_tapajos_km0810'

#Station 6
#comid_1 = 9023382
#comid_2 = 621030902
#name = 'Amazonas_uaupes_km2380'

#Station 7
#comid_1 = 9047884
#comid_2 = 621109666
#name = 'Amazonas_xingu_km1020'

#Station 8
#comid_1 = 8017519
#comid_2 = 470499982
#name = 'Balkhash_ili_km0392'

#Station 11
#comid_1 = 12061937
#comid_2 = 220372327
#name = 'Danube_sava_km1327'

#Station 11
#comid_1 = 12050781
#comid_2 = 220720275
#name = 'Danube_tisza_km1617'

#Station 12
#comid_1 = 12014938
#comid_2 = 220276772
#name = 'Dniepr_dnepr_km1390'

#Station 13
#comid_1 = 5025920
#comid_2 = 441167304
#name = 'Ganges-Brahmaputra_brahmaputra_km0809'

#Station 14
#comid_1 = 5031640
#comid_2 = 441185243
#name = 'Ganges-Brahmaputra_ganges_km1381'

#Station 15
#comid_1 = 5018188
#comid_2 = 441292437
#name = 'Indus_indus_km0949'

#Station 16
#comid_1 = 600366
#comid_2 = 220527770
#name = 'Kuban_kuban_km0397'

#Station 17
#comid_1 = 7050298
#comid_2 = 180234968
#name = 'Lake-Chad_logone_km0500'

#Station 18
#comid_1 = 13061982
#comid_2 = 760583077
#name = 'Mississippi_mississippi_km2378'

#Station 19
#comid_1 = 258721
#comid_2 = 540893378
#name = 'Murray_murray_km0651'

#Station 20
#comid_1 = 13009292
#comid_2 = 720111586
#name = 'Nass_nass_km0058'

#Station 21
#comid_1 = 7054410
#comid_2 = 140759702
#name = 'Niger_benue_km1000'

#Station 21
#comid_1 = 7055650
#comid_2 = 160521695
#name = 'Nile_baro_km4616'

#Station 22
#comid_1 = 947104
#comid_2 = 770368864
#name = 'Papaloapan_san-Juan_km0134'

#Station 23
#comid_1 = 9096566
#comid_2 = 640458755
#name = 'Parana_paraguai_km2622'

#Station 24
#comid_1 = 9085273
#comid_2 = 640101447
#name = 'Parana_paraguai_km3506'

#Station 25
#comid_1 = 9080933
#comid_2 = 630202025
#name = 'Sao-Francisco_sao-Francisco_km1577'

#Station 26
#comid_1 = 1018826
#comid_2 = 530187786
#name = 'Sepik_sepik_km0150'

#Station 28
#comid_1 = 1012389
#comid_2 = 520337491
#name = 'Sungai-Ketapang_sungai-Ketapang_km0121'

#Station 29
#comid_1 = 10049455
#comid_2 = 280354418
#name = 'Terek_terek_km0150'

#Station 30
#comid_1 = 10009772
#comid_2 = 280758239
#name = 'Volga_kliaz-Ma_km2504'

#Station 31
#comid_1 = 4061319
#comid_2 = 420746965
#name = 'Yangtze_chang-Jiang_km1426'

#Getting Historical (Retrospective Simulation)
#Version 1
era_res_v1 =  requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id=' + str(comid_1) + '&return_format=csv', verify=False).content
simulated_v1 = pd.read_csv(io.StringIO(era_res_v1.decode('utf-8')), index_col=0)
simulated_v1[simulated_v1 < 0] = 0
simulated_v1.index = pd.to_datetime(simulated_v1.index)
simulated_v1.index = simulated_v1.index.to_series().dt.strftime("%Y-%m-%d")
simulated_v1.index = pd.to_datetime(simulated_v1.index)
simulated_v1 = simulated_v1.loc[simulated_v1.index <= pd.to_datetime("2022-05-31")]

#Version 2
era_res_v2 =  requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivedaily/' + str(comid_2) + '?format=csv', verify=False).content
simulated_v2 = pd.read_csv(io.StringIO(era_res_v2.decode('utf-8')), index_col=0)
simulated_v2[simulated_v2 < 0] = 0
simulated_v2.index = pd.to_datetime(simulated_v2.index)
simulated_v2.index = simulated_v2.index.to_series().dt.strftime("%Y-%m-%d")
simulated_v2.index = pd.to_datetime(simulated_v2.index)

#Calculating the Return Period
#Version 1
rperiods_v1 = return_period_values(simulated_v1, comid_1)
rperiods_v1 = rperiods_v1.T

#Version 2
rperiods_v2 = return_period_values(simulated_v2, comid_2)
rperiods_v2 = rperiods_v2.T

#Getting Forecast Record
#Version 1
file_path_v1 = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_1)
if os.path.exists(file_path_v1):
    forecast_record_v1 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_1), index_col=0)
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
    forecast_record_v1.index = forecast_record_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
else:
    forecast_record_v1 = get_forecast_records_v1(comid_1, "20250504")
    forecast_record_v1[forecast_record_v1 < 0] = 0
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
    forecast_record_v1.index = forecast_record_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
    forecast_record_v1.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_1))

#Version 2
file_path_v2 = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_2)
if os.path.exists(file_path_v2):
    forecast_record_v2 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_2), index_col=0)
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
    forecast_record_v2.index = forecast_record_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
else:
    forecast_record_v2 = get_forecast_records_v2(comid_2, "20250504")
    forecast_record_v2[forecast_record_v2 < 0] = 0
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
    forecast_record_v2.index = forecast_record_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
    forecast_record_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_2))

#Get Forecast Data
#Version 1
url_1_v1 = 'https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={0}&date=20250515&ensemble=1-52&return_format=csv'.format(comid_1)
s = requests.get(url_1_v1, verify=False).content
df1_v1 = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
df1_v1[df1_v1 < 0] = 0
df1_v1.index = pd.to_datetime(df1_v1.index)
df1_v1.index = df1_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
df1_v1.index = pd.to_datetime(df1_v1.index)
df1_v1.rename(columns={"ensemble_01_m^3/s": "ensemble_01", "ensemble_02_m^3/s": "ensemble_02", "ensemble_03_m^3/s": "ensemble_03", "ensemble_04_m^3/s": "ensemble_04",
                       "ensemble_05_m^3/s": "ensemble_05", "ensemble_06_m^3/s": "ensemble_06", "ensemble_07_m^3/s": "ensemble_07", "ensemble_08_m^3/s": "ensemble_08",
                       "ensemble_09_m^3/s": "ensemble_09", "ensemble_10_m^3/s": "ensemble_10", "ensemble_11_m^3/s": "ensemble_11", "ensemble_12_m^3/s": "ensemble_12",
                       "ensemble_13_m^3/s": "ensemble_13", "ensemble_14_m^3/s": "ensemble_14", "ensemble_15_m^3/s": "ensemble_15", "ensemble_16_m^3/s": "ensemble_16",
                       "ensemble_17_m^3/s": "ensemble_17", "ensemble_18_m^3/s": "ensemble_18", "ensemble_19_m^3/s": "ensemble_19", "ensemble_20_m^3/s": "ensemble_20",
                       "ensemble_21_m^3/s": "ensemble_21", "ensemble_22_m^3/s": "ensemble_22", "ensemble_23_m^3/s": "ensemble_23", "ensemble_24_m^3/s": "ensemble_24",
                       "ensemble_25_m^3/s": "ensemble_25", "ensemble_26_m^3/s": "ensemble_26", "ensemble_27_m^3/s": "ensemble_27", "ensemble_28_m^3/s": "ensemble_28",
                       "ensemble_29_m^3/s": "ensemble_29", "ensemble_30_m^3/s": "ensemble_30", "ensemble_31_m^3/s": "ensemble_31", "ensemble_32_m^3/s": "ensemble_32",
                       "ensemble_33_m^3/s": "ensemble_33", "ensemble_34_m^3/s": "ensemble_34", "ensemble_35_m^3/s": "ensemble_35", "ensemble_36_m^3/s": "ensemble_36",
                       "ensemble_37_m^3/s": "ensemble_37", "ensemble_38_m^3/s": "ensemble_38", "ensemble_39_m^3/s": "ensemble_39", "ensemble_40_m^3/s": "ensemble_40",
                       "ensemble_41_m^3/s": "ensemble_41", "ensemble_42_m^3/s": "ensemble_42", "ensemble_43_m^3/s": "ensemble_43", "ensemble_44_m^3/s": "ensemble_44",
                       "ensemble_45_m^3/s": "ensemble_45", "ensemble_46_m^3/s": "ensemble_46", "ensemble_47_m^3/s": "ensemble_47", "ensemble_48_m^3/s": "ensemble_48",
                       "ensemble_49_m^3/s": "ensemble_49", "ensemble_50_m^3/s": "ensemble_50", "ensemble_51_m^3/s": "ensemble_51", "ensemble_52_m^3/s": "ensemble_52"},
              inplace=True)

forecast_stats_1_v1 = forecast_stats(df1_v1)

url_2_v1 = 'https://geoglows.ecmwf.int/api/ForecastEnsembles/?reach_id={0}&date=20250615&ensemble=1-52&return_format=csv'.format(comid_1)
s = requests.get(url_2_v1, verify=False).content
df2_v1 = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
df2_v1[df2_v1 < 0] = 0
df2_v1.index = pd.to_datetime(df2_v1.index)
df2_v1.index = df2_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
df2_v1.index = pd.to_datetime(df2_v1.index)
df2_v1.rename(columns={"ensemble_01_m^3/s": "ensemble_01", "ensemble_02_m^3/s": "ensemble_02", "ensemble_03_m^3/s": "ensemble_03", "ensemble_04_m^3/s": "ensemble_04",
                       "ensemble_05_m^3/s": "ensemble_05", "ensemble_06_m^3/s": "ensemble_06", "ensemble_07_m^3/s": "ensemble_07", "ensemble_08_m^3/s": "ensemble_08",
                       "ensemble_09_m^3/s": "ensemble_09", "ensemble_10_m^3/s": "ensemble_10", "ensemble_11_m^3/s": "ensemble_11", "ensemble_12_m^3/s": "ensemble_12",
                       "ensemble_13_m^3/s": "ensemble_13", "ensemble_14_m^3/s": "ensemble_14", "ensemble_15_m^3/s": "ensemble_15", "ensemble_16_m^3/s": "ensemble_16",
                       "ensemble_17_m^3/s": "ensemble_17", "ensemble_18_m^3/s": "ensemble_18", "ensemble_19_m^3/s": "ensemble_19", "ensemble_20_m^3/s": "ensemble_20",
                       "ensemble_21_m^3/s": "ensemble_21", "ensemble_22_m^3/s": "ensemble_22", "ensemble_23_m^3/s": "ensemble_23", "ensemble_24_m^3/s": "ensemble_24",
                       "ensemble_25_m^3/s": "ensemble_25", "ensemble_26_m^3/s": "ensemble_26", "ensemble_27_m^3/s": "ensemble_27", "ensemble_28_m^3/s": "ensemble_28",
                       "ensemble_29_m^3/s": "ensemble_29", "ensemble_30_m^3/s": "ensemble_30", "ensemble_31_m^3/s": "ensemble_31", "ensemble_32_m^3/s": "ensemble_32",
                       "ensemble_33_m^3/s": "ensemble_33", "ensemble_34_m^3/s": "ensemble_34", "ensemble_35_m^3/s": "ensemble_35", "ensemble_36_m^3/s": "ensemble_36",
                       "ensemble_37_m^3/s": "ensemble_37", "ensemble_38_m^3/s": "ensemble_38", "ensemble_39_m^3/s": "ensemble_39", "ensemble_40_m^3/s": "ensemble_40",
                       "ensemble_41_m^3/s": "ensemble_41", "ensemble_42_m^3/s": "ensemble_42", "ensemble_43_m^3/s": "ensemble_43", "ensemble_44_m^3/s": "ensemble_44",
                       "ensemble_45_m^3/s": "ensemble_45", "ensemble_46_m^3/s": "ensemble_46", "ensemble_47_m^3/s": "ensemble_47", "ensemble_48_m^3/s": "ensemble_48",
                       "ensemble_49_m^3/s": "ensemble_49", "ensemble_50_m^3/s": "ensemble_50", "ensemble_51_m^3/s": "ensemble_51", "ensemble_52_m^3/s": "ensemble_52"},
              inplace=True)

forecast_stats_2_v1 = forecast_stats(df2_v1)

#Version 2
url_1_v2 = 'https://geoglows.ecmwf.int/api/v2/forecastensemble/{0}?date=20250515&format=csv'.format(comid_2)
s = requests.get(url_1_v2, verify=False).content
df1_v2 = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
df1_v2[df1_v2 < 0] = 0
df1_v2.index = pd.to_datetime(df1_v2.index)
df1_v2.index = df1_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
df1_v2.index = pd.to_datetime(df1_v2.index)

forecast_stats_1_v2 = forecast_stats(df1_v2)

url_2_v2 = 'https://geoglows.ecmwf.int/api/v2/forecastensemble/{0}?date=20250615&format=csv'.format(comid_2)
s = requests.get(url_2_v2, verify=False).content
df2_v2 = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
df2_v2[df2_v2 < 0] = 0
df2_v2.index = pd.to_datetime(df2_v2.index)
df2_v2.index = df2_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
df2_v2.index = pd.to_datetime(df2_v2.index)

forecast_stats_2_v2 = forecast_stats(df2_v2)

forecast_record_1_v1 = forecast_record_v1.loc[forecast_record_v1.index >= pd.to_datetime(forecast_stats_1_v1.index[0] - dt.timedelta(days=9))]
forecast_record_1_v1 = forecast_record_1_v1.loc[forecast_record_1_v1.index <= pd.to_datetime(forecast_stats_1_v1.index[0])]

forecast_record_2_v1 = forecast_record_v1.loc[forecast_record_v1.index >= pd.to_datetime(forecast_stats_2_v1.index[0] - dt.timedelta(days=5))]
forecast_record_2_v1 = forecast_record_2_v1.loc[forecast_record_2_v1.index <= pd.to_datetime(forecast_stats_2_v1.index[0])]

forecast_record_1_v2 = forecast_record_v2.loc[forecast_record_v2.index >= pd.to_datetime(forecast_stats_1_v2.index[0] - dt.timedelta(days=9))]
forecast_record_1_v2 = forecast_record_1_v2.loc[forecast_record_1_v2.index <= pd.to_datetime(forecast_stats_1_v2.index[0])]

forecast_record_2_v2 = forecast_record_v2.loc[forecast_record_v2.index >= pd.to_datetime(forecast_stats_2_v2.index[0] - dt.timedelta(days=5))]
forecast_record_2_v2 = forecast_record_2_v2.loc[forecast_record_2_v2.index <= pd.to_datetime(forecast_stats_2_v2.index[0])]

# Creating a 2x2 grid of subplots
fig, axs = plt.subplots(2, 2, figsize=(15, 10))

#forecast_stats_records_matplotlib(ax=axs[0, 0], df=forecast_stats_1_v1, rp_df = rperiods_v1, records_df=forecast_record_1_v1)
forecast_stats_records_matplotlib(ax=axs[0, 0], df=forecast_stats_1_v1, records_df=forecast_record_1_v1)
#forecast_stats_records_matplotlib(ax=axs[0, 0], df=forecast_stats_1_v1, rp_df = rperiods_v1)

#forecast_stats_records_matplotlib(ax=axs[0, 1], df=forecast_stats_2_v1, rp_df = rperiods_v1, records_df=forecast_record_2_v1)
forecast_stats_records_matplotlib(ax=axs[0, 1], df=forecast_stats_2_v1, records_df=forecast_record_2_v1)
#forecast_stats_records_matplotlib(ax=axs[0, 1], df=forecast_stats_2_v1, rp_df = rperiods_v1)

#forecast_stats_records_matplotlib(ax=axs[1, 0], df=forecast_stats_1_v2, rp_df = rperiods_v2, records_df=forecast_record_1_v2)
forecast_stats_records_matplotlib(ax=axs[1, 0], df=forecast_stats_1_v2, records_df=forecast_record_1_v2)
#forecast_stats_records_matplotlib(ax=axs[1, 0], df=forecast_stats_1_v2, rp_df = rperiods_v2)

#forecast_stats_records_matplotlib(ax=axs[1, 1], df=forecast_stats_2_v2, rp_df = rperiods_v2, records_df=forecast_record_2_v2)
forecast_stats_records_matplotlib(ax=axs[1, 1], df=forecast_stats_2_v2, records_df=forecast_record_2_v2)
#forecast_stats_records_matplotlib(ax=axs[1, 1], df=forecast_stats_2_v2, rp_df = rperiods_v2)

# Setting x-axis range
axs[0, 0].set_xlim(forecast_record_1_v1.index[0], forecast_stats_1_v1.index[-1])
axs[0, 1].set_xlim(forecast_record_2_v1.index[0], forecast_stats_2_v1.index[-1])
axs[1, 0].set_xlim(forecast_record_1_v1.index[0], forecast_stats_1_v1.index[-1])
axs[1, 1].set_xlim(forecast_record_2_v1.index[0], forecast_stats_2_v1.index[-1])

plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots\\Forecast Comparison {}.png'.format(name), dpi=700)

# Show the plots
plt.show()

print(rperiods_v1)
print(rperiods_v2)