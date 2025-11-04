import io
import os
import math
import math
import requests
import numpy as np
import pandas as pd
import xarray as xr
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

def get_forecast_records_glofas(lat, lon, start_date):
    """
    Fetches forecast data from a given start date to today, gets the first value of the glofas forecast,
    which corresponds to the 1-day Forecast Value.

    Args:
        get_forecast_records (function): A function that retrieves forecast records for a given date and latitude and longitude.
        lat (float): The glofas latitude for the river of interest.
        lon (float): The glofas longitude for the river of interest.
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

        try:
            forecast_data = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}_{4}.csv".format(yyyy,mm,dd,lat,lon), index_col=0)
            forecast_data.index = pd.to_datetime(forecast_data.index)
            forecast_data[forecast_data < 0] = 0
            forecast_data.index = forecast_data.index.to_series().dt.strftime("%Y-%m-%d")
            forecast_data.index = pd.to_datetime(forecast_data.index)
            forecast_data.dropna(inplace=True)

            mean_df = forecast_data.mean(axis=1).to_frame()
            mean_df.rename(columns = {0:'average_flow'}, inplace = True)

            if isinstance(mean_df, pd.DataFrame):
                # Extract the first 8 time steps
                summary_df = mean_df.iloc[:1]
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

def forecast_stats_records_matplotlib(ax, df: pd.DataFrame, *, rp_df: pd.DataFrame = None, records_df: pd.DataFrame = None, retro_df: pd.DataFrame = None, plot_titles: list = None, show_maxmin: bool = False):
    """
    Makes the streamflow data and optional metadata into a matplotlib plot

    Args:
        ax: Matplotlib axis object where the figure will be drawn.
        df: the csv response from forecast_stats
        rp_df: the csv response from return_periods
        records_df: dataframe containing forecast record data to be added to the plot
        retro_df: dataframe containing historical data to be added to the plot
        plot_titles: a list of strings to place in the figure title. each list item will be on a new line.
        show_maxmin: Choose to show or hide the max/min envelope by default
    """

    # --- Determine available columns safely ---
    has_high_res = 'high_res' in df.columns

    # Common dates for percentiles and average
    if 'flow_avg' in df.columns:
        dates_stats = df['flow_avg'].dropna().index.tolist()
    else:
        raise ValueError("The DataFrame must contain a 'flow_avg' column.")

    # If available, get high-res dates
    dates_high_res = df['high_res'].dropna().index.tolist() if has_high_res else []

    # Prepare the plot data dynamically (only existing columns)
    plot_data = {}
    for col in ['flow_max', 'flow_75p', 'flow_avg', 'flow_med', 'flow_25p', 'flow_min']:
        if col in df.columns:
            plot_data[col] = df[col].dropna()

    if has_high_res:
        plot_data['high_res'] = df['high_res'].dropna()

    # Create the figure
    #fig, ax = plt.subplots(figsize=(10, 6))

    # Plot max/min envelope if required
    # if show_maxmin:
    #    ax.fill_between(dates_stats, plot_data['flow_max'], plot_data['flow_min'], color='lightblue', label='Maximum & Minimum Flow')

    ax.fill_between(dates_stats, plot_data['flow_max'], plot_data['flow_min'], color='lightblue',label='Maximum & Minimum Flow', edgecolor='black', linestyle='--')

    # Plot percentiles
    ax.fill_between(dates_stats, plot_data['flow_75p'], plot_data['flow_25p'], color='lightgreen',label='25-75 Percentile Flow', edgecolor='green')

    # Plot other data
    ax.plot(dates_stats, plot_data['flow_avg'], label='Average Flow', color='blue')
    ax.plot(dates_stats, plot_data['flow_med'], label='Median Flow', color='red')
    if has_high_res and len(dates_high_res) > 0:
        ax.plot(dates_high_res, plot_data['high_res'], label='High-Resolution Forecast', color='black')

    # Plot records_df if provided
    if records_df is not None and len(records_df) > 1:
        ax.plot(records_df.index, records_df.iloc[:, 0].values, label='1st day Forecast', color='#FFA15A')

    # Plot records_df if provided
    if retro_df is not None and len(retro_df) > 0:
        ax.plot(retro_df.index, retro_df.iloc[:, 0].values, label='Retrospective', color='#1f77b4')

    if len(df.index) > 15:
        min_date = pd.to_datetime(df.index[0] - dt.timedelta(days=15))
        max_date = pd.to_datetime(df.index[0] + dt.timedelta(days=15))
    else:
        min_date = pd.to_datetime(df.index[0] - dt.timedelta(days=16))
        max_date = pd.to_datetime(df.index[0] + dt.timedelta(days=14))

    if rp_df is not None and len(rp_df) > 0:
        y_max = max(df['flow_max'])
        r2 = int(rp_df[2].values[0])
        r5 = int(rp_df[5].values[0])
        r10 = int(rp_df[10].values[0])
        r25 = int(rp_df[25].values[0])
        r50 = int(rp_df[50].values[0])
        r100 = int(rp_df[100].values[0])
        rmax = int(max(r100 + (r100*0.05), y_max))

        ax.fill_between([min_date, max_date], [r100 * 0.05, r100 * 0.05], [r100 * 0.05, r100 * 0.05], color=(0 / 255, 0 / 255, 0 / 255, 0), label='Return Periods')
        ax.fill_between([min_date, max_date], [r2, r2], [r5, r5], color=(254 / 255, 240 / 255, 1 / 255, 0.4), label='2 Year: {}'.format(r2), edgecolor=(254 / 255, 240 / 255, 1 / 255))
        ax.fill_between([min_date, max_date], [r5, r5], [r10, r10], color=(253 / 255, 154 / 255, 1 / 255, 0.4), label='5 Year: {}'.format(r5), edgecolor=(253 / 255, 154 / 255, 1 / 255))
        ax.fill_between([min_date, max_date], [r10, r10], [r25, r25], color=(255 / 255, 56 / 255, 5 / 255, 0.4), label='10 Year: {}'.format(r10), edgecolor=(255 / 255, 56 / 255, 5 / 255))
        ax.fill_between([min_date, max_date], [r25, r25], [r50, r50], color=(255 / 255, 0 / 255, 0 / 255, 0.4), label='25 Year: {}'.format(r25), edgecolor=(255 / 255, 0 / 255, 0 / 255))
        ax.fill_between([min_date, max_date], [r50, r50], [r100, r100], color=(128 / 255, 0 / 255, 106 / 255, 0.4), label='50 Year: {}'.format(r50), edgecolor=(128 / 255, 0 / 255, 106 / 255))
        ax.fill_between([min_date, max_date], [r100, r100], [rmax, rmax], color=(128 / 255, 0 / 255, 246 / 255, 0.4), label='100 Year: {}'.format(r100), edgecolor=(128 / 255, 0 / 255, 246 / 255))

        #if records_df is not None and len(records_df) > 0:
        #    ax.fill_between([records_df.index[0], dates_stats[-1]], [r100 * 0.05, r100 * 0.05], [r100 * 0.05, r100 * 0.05], color=(0 / 255, 0 / 255, 0 / 255, 0), label='Return Periods')
        #    ax.fill_between([records_df.index[0], dates_stats[-1]], [r2, r2], [r5, r5], color=(254 / 255, 240 / 255, 1 / 255, 0.4), label='2 Year: {}'.format(r2), edgecolor=(254 / 255, 240 / 255, 1 / 255))
        #    ax.fill_between([records_df.index[0], dates_stats[-1]], [r5, r5], [r10, r10], color=(253 / 255, 154 / 255, 1 / 255, 0.4), label='5 Year: {}'.format(r5), edgecolor=(253 / 255, 154 / 255, 1 / 255))
        #    ax.fill_between([records_df.index[0], dates_stats[-1]], [r10, r10], [r25, r25], color=(255 / 255, 56 / 255, 5 / 255, 0.4), label='10 Year: {}'.format(r10), edgecolor=(255 / 255, 56 / 255, 5 / 255))
        #    ax.fill_between([records_df.index[0], dates_stats[-1]], [r25, r25], [r50, r50], color=(255 / 255, 0 / 255, 0 / 255, 0.4), label='25 Year: {}'.format(r25), edgecolor=(255 / 255, 0 / 255, 0 / 255))
        #    ax.fill_between([records_df.index[0], dates_stats[-1]], [r50, r50], [r100, r100], color=(128 / 255, 0 / 255, 106 / 255, 0.4), label='50 Year: {}'.format(r50), edgecolor=(128 / 255, 0 / 255, 106 / 255))
        #    ax.fill_between([records_df.index[0], dates_stats[-1]], [r100, r100], [rmax, rmax], color=(128 / 255, 0 / 255, 246 / 255, 0.4), label='100 Year: {}'.format(r100), edgecolor=(128 / 255, 0 / 255, 246 / 255))
        #else:
        #    ax.fill_between([dates_stats[0], dates_stats[-1]], [r100 * 0.05, r100 * 0.05], [r100 * 0.05, r100 * 0.05], color=(0 / 255, 0 / 255, 0 / 255, 0), label='Return Periods')
        #    ax.fill_between([dates_stats[0], dates_stats[-1]], [r2, r2], [r5, r5], color=(254 / 255, 240 / 255, 1 / 255, 0.4), label='2 Year: {}'.format(r2), edgecolor=(254 / 255, 240 / 255, 1 / 255))
        #    ax.fill_between([dates_stats[0], dates_stats[-1]], [r5, r5], [r10, r10], color=(253 / 255, 154 / 255, 1 / 255, 0.4), label='5 Year: {}'.format(r5), edgecolor=(253 / 255, 154 / 255, 1 / 255))
        #    ax.fill_between([dates_stats[0], dates_stats[-1]], [r10, r10], [r25, r25], color=(255 / 255, 56 / 255, 5 / 255, 0.4), label='10 Year: {}'.format(r10), edgecolor=(255 / 255, 56 / 255, 5 / 255))
        #    ax.fill_between([dates_stats[0], dates_stats[-1]], [r25, r25], [r50, r50], color=(255 / 255, 0 / 255, 0 / 255, 0.4), label='25 Year: {}'.format(r25), edgecolor=(255 / 255, 0 / 255, 0 / 255))
        #    ax.fill_between([dates_stats[0], dates_stats[-1]], [r50, r50], [r100, r100], color=(128 / 255, 0 / 255, 106 / 255, 0.4), label='50 Year: {}'.format(r50), edgecolor=(128 / 255, 0 / 255, 106 / 255))
        #    ax.fill_between([dates_stats[0], dates_stats[-1]], [r100, r100], [rmax, rmax], color=(128 / 255, 0 / 255, 246 / 255, 0.4), label='100 Year: {}'.format(r100), edgecolor=(128 / 255, 0 / 255, 246 / 255))

    # Dynamically setting x-ticks
    #if records_df is not None and len(records_df) > 0:
    #    ax.set_xlim(records_df.index[0], dates_stats[-1])
    #    ax.set_xticks(pd.date_range(start=records_df.index[0], end=dates_stats[-1], periods=6))
    #else:
    #    # Only forecast dates
    #    ax.set_xlim(dates_stats[0], dates_stats[-1])
    #    ax.set_xticks(pd.date_range(start=dates_stats[0], end=dates_stats[-1], periods=5))

    ax.set_xlim(min_date, max_date)
    ax.set_xticks(pd.date_range(start=min_date, end=max_date, periods=5))

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

    # Determinar si existe la columna ensemble_52
    has_high_res = 'ensemble_52' in ensemble.columns

    if has_high_res:
        high_res_df = ensemble['ensemble_52'].to_frame()
        ensemble = ensemble.drop(columns=['ensemble_52'])
        high_res_df.dropna(inplace=True)
    else:
        high_res_df = None

    # Eliminar filas completamente vacías
    ensemble.dropna(how='all', inplace=True)

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

    # Combinar resultados
    if has_high_res:
        high_res_df.rename(columns={'ensemble_52': 'high_res'}, inplace=True)
        forecast_stats_df = pd.concat([max_df, p75_df, mean_df, p50_df, p25_df, min_df, high_res_df], axis=1)
    else:
        forecast_stats_df = pd.concat([max_df, p75_df, mean_df, p50_df, p25_df, min_df], axis=1)

    return forecast_stats_df

#fechas = ['20250610', '20250611', '20250612', '20250613', '20250614', '20250615', '20250616', '20250617', '20250618', '20250619',
#          '20250620', '20250621', '20250622', '20250623', '20250624', '20250625', '20250626', '20250627', '20250628', '20250629',
#          '20250630', '20250701', '20250702', '20250703', '20250704', '20250705', '20250706', '20250707', '20250708', '20250709',
#          '20250710', '20250711', '20250712', '20250713', '20250714', '20250715', '20250716', '20250717', '20250718', '20250719',
#          '20250720', '20250721', '20250722', '20250723', '20250724', '20250725', '20250726', '20250727', '20250728', '20250729',
#          '20250730', '20250731', '20250801', '20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808',
#          '20250809', '20250810', '20250811', '20250812', '20250813', '20250814', '20250815', '20250816', '20250817', '20250818',
#          '20250819', '20250820', '20250821', '20250822', '20250823', '20250824', '20250825', '20250826', '20250827', '20250828',
#          '20250829', '20250830', '20250831', '20250901', '20250902', '20250903', '20250904', '20250905', '20250906', '20250907',
#          '20250908', '20250909', '20250910', '20250911', '20250912', '20250913', '20250914', '20250915', '20250916', '20250917',
#          '20250918', '20250919', '20250920', '20250921', '20250922', '20250923', '20250924', '20250925', '20250926', '20250927',
#          '20250928', '20250929', '20250930', '20251001', '20251002', '20251003', '20251004', '20251005', '20251006', '20251007',
#          '20251008', '20251009', '20251010', '20251011', '20251012', '20251013', '20251014']

fechas = ['20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808',
          '20250809', '20250810', '20250811', '20250812', '20250813', '20250814', '20250815', '20250816', '20250817', '20250818',
          '20250819', '20250820', '20250821', '20250822', '20250823', '20250824', '20250825', '20250826', '20250827', '20250828',
          '20250829', '20250830', '20250831', '20250901', '20250902', '20250903', '20250904', '20250905', '20250906', '20250907',
          '20250908', '20250909', '20250910', '20250911', '20250912', '20250913', '20250914', '20250915', '20250916', '20250917',
          '20250918', '20250919', '20250920', '20250921', '20250922', '20250923', '20250924', '20250925', '20250926', '20250927',
          '20250928', '20250929', '20250930', '20251001', '20251002', '20251003', '20251004', '20251005', '20251006', '20251007',
          '20251008', '20251009', '20251010', '20251011', '20251012', '20251013', '20251014', '20251015']

stations = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison_v1.csv")
comid_1s = stations['COMID_v1'].to_list()
comid_2s = stations['COMID_v2'].to_list()
latitudes = stations['Lat_GloFAS'].to_list()
longitudes = stations['Lon_GloFAS'].to_list()
names = stations['name'].to_list()

for fecha in fechas:

    print(fecha[0:4], '-', fecha[4:6], '-',  fecha[6:8])

    for comid_1, comid_2, name, latitude, longitude in zip(comid_1s, comid_2s, names, latitudes, longitudes):

        forecast_stats_v1 = pd.DataFrame()
        forecast_stats_v2 = pd.DataFrame()
        forecast_stats_glofas = pd.DataFrame()
        forecast_record_v1 = pd.DataFrame()
        forecast_record_v2 = pd.DataFrame()
        forecast_record_glofas = pd.DataFrame()

        #Getting Historical (Retrospective Simulation)
        # Version 1
        # era_res_v1 =  requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id=' + str(comid_1) + '&return_format=csv', verify=False).content
        # simulated_v1 = pd.read_csv(io.StringIO(era_res_v1.decode('utf-8')), index_col=0)
        simulated_v1 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GEOGLOWS_v1\\{}.csv".format(comid_1), index_col=0)
        simulated_v1[simulated_v1 < 0] = 0
        simulated_v1.index = pd.to_datetime(simulated_v1.index)
        simulated_v1.index = simulated_v1.index.to_series().dt.strftime("%Y-%m-%d")
        simulated_v1.index = pd.to_datetime(simulated_v1.index)
        simulated_v1 = simulated_v1.loc[simulated_v1.index <= pd.to_datetime("2022-05-31")]

        #Version 2
        # era_res_v2 = requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivedaily/' + str(comid_2) + '?format=csv', verify=False).content
        # era_res_v2 = requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivehourly/' + str(comid_2) + '?format=csv', verify=False).content
        # simulated_v2 = pd.read_csv(io.StringIO(era_res_v2.decode('utf-8')), index_col=0)
        simulated_v2 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GEOGLOWS_v2\\{}.csv".format(comid_2), index_col=0)
        simulated_v2[simulated_v2 < 0] = 0
        simulated_v2.index = pd.to_datetime(simulated_v2.index)
        # simulated_v2.index = simulated_v2.index.to_series().dt.strftime("%Y-%m-%d")
        simulated_v2.index = simulated_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        simulated_v2.index = pd.to_datetime(simulated_v2.index)
        # simulated_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GEOGLOWS_v2\\{}.csv".format(comid_2))

        # Calculating the Return Period
        # Version 1
        rperiods_v1 = return_period_values(simulated_v1, comid_1)
        rperiods_v1 = rperiods_v1.T
        # rperiods_v1.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Return_Periods\\GEOGLOWS_v1\\{}.csv".format(comid_1))

        # Version 2
        rperiods_v2 = return_period_values(simulated_v2, comid_2)
        rperiods_v2 = rperiods_v2.T
        # rperiods_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Return_Periods\\GEOGLOWS_v2\\{}.csv".format(comid_2))

        # GloFAS
        simulated_glofas = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GloFAS\\{0}_{1}.csv".format(latitude, longitude), index_col=0)
        simulated_glofas.index = pd.to_datetime(simulated_glofas.index)
        simulated_glofas.index = simulated_glofas.index.to_series().dt.strftime("%Y-%m-%d")
        simulated_glofas.index = pd.to_datetime(simulated_glofas.index)
        rperiods_glofas = return_period_values(simulated_glofas, "{0} {1}".format(latitude, longitude))
        rperiods_glofas = rperiods_glofas.T
        # rperiods_glofas.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Return_Periods\\GloFAS\\{0}_{1}.csv".format(latitude, longitude))


        #Getting Forecast Record
        #Version 1
        file_path_v1 = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v1\\{0}.csv".format(comid_1)
        if os.path.exists(file_path_v1):
            forecast_record_v1 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v1\\{0}.csv".format(comid_1), index_col=0)
            forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
            forecast_record_v1.index = forecast_record_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
        else:
            forecast_record_v1 = get_forecast_records_v1(comid_1, "20250610")
            forecast_record_v1[forecast_record_v1 < 0] = 0
            forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
            forecast_record_v1.index = forecast_record_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
            forecast_record_v1.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v1\\{0}.csv".format(comid_1))
        
        #Version 2
        file_path_v2 = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v2\\{0}.csv".format(comid_2)
        if os.path.exists(file_path_v2):
            forecast_record_v2 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v2\\{0}.csv".format(comid_2), index_col=0)
            forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
            forecast_record_v2.index = forecast_record_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
        else:
            forecast_record_v2 = get_forecast_records_v2(comid_2, "20250610")
            forecast_record_v2[forecast_record_v2 < 0] = 0
            forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
            forecast_record_v2.index = forecast_record_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
            forecast_record_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v2\\{0}.csv".format(comid_2))

        # GloFAS
        file_path_glofas = "G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GloFAS\\{0}_{1}.csv".format(latitude, longitude)
        if os.path.exists(file_path_glofas):
            forecast_record_glofas = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GloFAS\\{0}_{1}.csv".format(latitude, longitude), index_col=0)
            forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)
            forecast_record_glofas.index = forecast_record_glofas.index.to_series().dt.strftime("%Y-%m-%d")
            forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)
        else:
            forecast_record_glofas = get_forecast_records_glofas(latitude, longitude, '20250610')
            forecast_record_glofas[forecast_record_glofas < 0] = 0
            forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)
            forecast_record_glofas.index = forecast_record_glofas.index.to_series().dt.strftime("%Y-%m-%d")
            forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)
            forecast_record_glofas.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GloFAS\\{0}_{1}.csv".format(latitude, longitude))

        #Get Forecast Data
        #Version 1
        try:
            df1_v1 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], comid_1), index_col=0)
            df1_v1[df1_v1 < 0] = 0
            df1_v1.index = pd.to_datetime(df1_v1.index)
            df1_v1.index = df1_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            df1_v1.index = pd.to_datetime(df1_v1.index)
            forecast_stats_v1 = forecast_stats(df1_v1)
        except Exception as e:
            print(e)

        #Version 2
        try:
            df1_v2 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], comid_2), index_col=0)
            df1_v2[df1_v2 < 0] = 0
            df1_v2.index = pd.to_datetime(df1_v2.index)
            df1_v2.index = df1_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            df1_v2.index = pd.to_datetime(df1_v2.index)
            forecast_stats_v2 = forecast_stats(df1_v2)
        except Exception as e:
            print(e)

        #GloFAS
        try:
            df1_glofas = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}_{4}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], latitude, longitude), index_col=0)
            df1_glofas[df1_glofas < 0] = 0
            df1_glofas.index = pd.to_datetime(df1_glofas.index)
            df1_glofas.index = df1_glofas.index.to_series().dt.strftime("%Y-%m-%d")
            df1_glofas.index = pd.to_datetime(df1_glofas.index)
            forecast_stats_glofas = forecast_stats(df1_glofas)
        except Exception as e:
            print(e)

        ###Forecast Record
        #Version 1
        try:
            forecast_record_plot_v1 = forecast_record_v1.loc[forecast_record_v1.index >= pd.to_datetime(forecast_stats_v1.index[0] - dt.timedelta(days=15))]
            forecast_record_plot_v1 = forecast_record_plot_v1.loc[forecast_record_plot_v1.index <= pd.to_datetime(forecast_stats_v1.index[0])]
        except Exception as e:
            print(e)

        # Version 2
        try:
            forecast_record_plot_v2 = forecast_record_v2.loc[forecast_record_v2.index >= pd.to_datetime(forecast_stats_v2.index[0] - dt.timedelta(days=15))]
            forecast_record_plot_v2 = forecast_record_plot_v2.loc[forecast_record_plot_v2.index <= pd.to_datetime(forecast_stats_v2.index[0])]
        except Exception as e:
            print(e)

        # GloFAS
        try:
            forecast_record_plot_glofas = forecast_record_glofas.loc[forecast_record_glofas.index >= pd.to_datetime(forecast_stats_glofas.index[0] - dt.timedelta(days=16))]
            forecast_record_plot_glofas = forecast_record_plot_glofas.loc[forecast_record_plot_glofas.index <= pd.to_datetime(forecast_stats_glofas.index[0])]
        except Exception as e:
            print(e)

        ###Retrospective
        # Version 1
        try:
            simulated_plot_v1 = simulated_v1.loc[simulated_v1.index >= pd.to_datetime(forecast_stats_v1.index[0] - dt.timedelta(days=15))]
            simulated_plot_v1 = simulated_plot_v1.loc[simulated_plot_v1.index <= pd.to_datetime(forecast_stats_v1.index[0])]
        except Exception as e:
            print(e)

        # Version 2
        try:
            simulated_plot_v2 = simulated_v2.loc[simulated_v2.index >= pd.to_datetime(forecast_stats_v2.index[0] - dt.timedelta(days=15))]
            simulated_plot_v2 = simulated_plot_v2.loc[simulated_plot_v2.index <= pd.to_datetime(forecast_stats_v2.index[0])]
        except Exception as e:
            print(e)

        # GloFAS
        try:
            simulated_plot_glofas = simulated_glofas.loc[simulated_glofas.index >= pd.to_datetime(forecast_stats_glofas.index[0] - dt.timedelta(days=16))]
            simulated_plot_glofas = simulated_plot_glofas.loc[simulated_plot_glofas.index <= pd.to_datetime(forecast_stats_glofas.index[0])]
        except Exception as e:
            print(e)

        if (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(3, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, rp_df=rperiods_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_v2, rp_df=rperiods_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles = [f'GEOGLOWS V2 Reach ID: {comid_2}'])
            forecast_stats_glofas_plot = forecast_stats_glofas.loc[forecast_stats_glofas.index <= pd.to_datetime(forecast_stats_v1.index[-1])]
            forecast_stats_records_matplotlib(ax=axs[2], df=forecast_stats_glofas_plot, rp_df=rperiods_glofas, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is None or len(forecast_stats_v1) == 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(2, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v2, rp_df=rperiods_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles=[f'GEOGLOWS V2 Reach ID: {comid_2}'])
            forecast_stats_glofas_plot = forecast_stats_glofas.loc[forecast_stats_glofas.index <= pd.to_datetime(forecast_stats_v2.index[-1] + dt.timedelta(days=1))]
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_glofas_plot, rp_df=rperiods_glofas, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is None or len(forecast_stats_v2) == 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(2, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, rp_df=rperiods_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            forecast_stats_glofas_plot = forecast_stats_glofas.loc[forecast_stats_glofas.index <= pd.to_datetime(forecast_stats_v1.index[-1])]
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_glofas_plot, rp_df=rperiods_glofas, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is None or len(forecast_stats_glofas) == 0):
            fig, axs = plt.subplots(2, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, rp_df=rperiods_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_v2, rp_df=rperiods_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles = [f'GEOGLOWS V2 Reach ID: {comid_2}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is None or len(forecast_stats_v1) == 0) and (forecast_stats_v2 is None or len(forecast_stats_v2) == 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(1, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_glofas, rp_df=rperiods_glofas, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is None or len(forecast_stats_v1) == 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is None or len(forecast_stats_glofas) == 0):
            fig, axs = plt.subplots(1, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v2, rp_df=rperiods_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles = [f'GEOGLOWS V2 Reach ID: {comid_2}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is None or len(forecast_stats_v2) == 0) and (forecast_stats_glofas is None or len(forecast_stats_glofas) == 0):
            fig, axs = plt.subplots(1, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, rp_df=rperiods_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        else:
            print("There is no forecast data to plot")
        plt.close(fig)


        if (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(3, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles = [f'GEOGLOWS V2 Reach ID: {comid_2}'])
            forecast_stats_glofas_plot = forecast_stats_glofas.loc[forecast_stats_glofas.index <= pd.to_datetime(forecast_stats_v1.index[-1])]
            forecast_stats_records_matplotlib(ax=axs[2], df=forecast_stats_glofas_plot, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN_NoReturnPeriods\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is None or len(forecast_stats_v1) == 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(2, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles=[f'GEOGLOWS V2 Reach ID: {comid_2}'])
            forecast_stats_glofas_plot = forecast_stats_glofas.loc[forecast_stats_glofas.index <= pd.to_datetime(forecast_stats_v2.index[-1] + dt.timedelta(days=1))]
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_glofas_plot, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN_NoReturnPeriods\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is None or len(forecast_stats_v2) == 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(2, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            forecast_stats_glofas_plot = forecast_stats_glofas.loc[forecast_stats_glofas.index <= pd.to_datetime(forecast_stats_v1.index[-1])]
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_glofas_plot, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN_NoReturnPeriods\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is None or len(forecast_stats_glofas) == 0):
            fig, axs = plt.subplots(2, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles = [f'GEOGLOWS V2 Reach ID: {comid_2}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN_NoReturnPeriods\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is None or len(forecast_stats_v1) == 0) and (forecast_stats_v2 is None or len(forecast_stats_v2) == 0) and (forecast_stats_glofas is not None and len(forecast_stats_glofas) > 0):
            fig, axs = plt.subplots(1, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_glofas, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {latitude}, {longitude}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN_NoReturnPeriods\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is None or len(forecast_stats_v1) == 0) and (forecast_stats_v2 is not None and len(forecast_stats_v2) > 0) and (forecast_stats_glofas is None or len(forecast_stats_glofas) == 0):
            fig, axs = plt.subplots(1, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v2, records_df=forecast_record_plot_v2, retro_df=simulated_plot_v2, plot_titles = [f'GEOGLOWS V2 Reach ID: {comid_2}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN_NoReturnPeriods\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        elif (forecast_stats_v1 is not None and len(forecast_stats_v1) > 0) and (forecast_stats_v2 is None or len(forecast_stats_v2) == 0) and (forecast_stats_glofas is None or len(forecast_stats_glofas) == 0):
            fig, axs = plt.subplots(1, 1, figsize=(15, 10))
            forecast_stats_records_matplotlib(ax=axs[0], df=forecast_stats_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
            plt.tight_layout(pad=2.5, h_pad=3.5)
            plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Plots_JimN_NoReturnPeriods\\{0}\\Forecast Comparison {0} {1}.png'.format(name, fecha), dpi=700)
        else:
            print("There is no forecast data to plot")
        plt.close(fig)