import io
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
    #end_date = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = dt.datetime(year=2024, month=11, day=10)

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
        #print(f'{yyyy}-{mm}-{dd} v1')

        try:
            file_location = 'G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values\\{0}\\row_data\\{1}-{2}-{3}.csv'.format(river_id, yyyy, mm, dd)
            forecast_data = pd.read_csv(file_location, index_col=0)
            forecast_data.index = pd.to_datetime(forecast_data.index)
            forecast_data[forecast_data < 0] = 0
            forecast_data.index = forecast_data.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
            forecast_data.index = pd.to_datetime(forecast_data.index)
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

def get_forecast_records_efas(lat, lon, start_date):
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
    #current_date = start_date
    #end_date = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = dt.datetime(year=2024, month=11, day=10)

    fechas = pd.date_range(start_date, end_date, freq='12H')
    fechas = fechas.strftime("%Y-%m-%d %H")

    for fecha in fechas:

        yyyy = str(int(fecha[0:4]))
        mm = str(int(fecha[5:7]))
        dd = str(fecha[8:10])
        hh = str(fecha[11:14])

        try:
            forecast_data = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values_EFAS\\{0}-{1}-{2} {3}\\{4}_{5}.csv".format(yyyy,mm,dd,hh,lat,lon), index_col=0)
            forecast_data.index = pd.to_datetime(forecast_data.index)
            forecast_data[forecast_data < 0] = 0
            forecast_data.index = forecast_data.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
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
    #end_date = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = dt.datetime(year=2024, month=11, day=10)

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
            forecast_data = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values_GloFAS\\{0}-{1}-{2}\\{3}_{4}.csv".format(yyyy,mm,dd,lat,lon), index_col=0)
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
    max_annual_flow.dropna(inplace=True)
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

    #Define Max Plot Value
    y_max = max(df['flow_max'])

    # Plot records_df if provided
    if records_df is not None and len(records_df) > 1:
        ax.plot(records_df.index, records_df.iloc[:, 0].values, label='1st day Forecast', color='#FFA15A')
        y_max = max(y_max,max(records_df.iloc[:, 0].values))

    # Plot records_df if provided
    if retro_df is not None and len(retro_df) > 0:
        ax.plot(retro_df.index, retro_df.iloc[:, 0].values, label='Retrospective', color='#1f77b4')
        y_max = max(y_max, max(retro_df.iloc[:, 0].values))

    if len(df.index) > 15:
        min_date = pd.to_datetime(df.index[0] - dt.timedelta(days=15))
        max_date = pd.to_datetime(df.index[0] + dt.timedelta(days=15))
    else:
        min_date = pd.to_datetime(df.index[0] - dt.timedelta(days=16))
        max_date = pd.to_datetime(df.index[0] + dt.timedelta(days=14))

    if rp_df is not None and len(rp_df) > 0:
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

def forecast_stats_geoglows (ensembles_df, high_res_df):
    ensemble = ensembles_df.copy()
    high_res_df.rename(columns={'ensemble_52_m^3/s': 'high_res'}, inplace=True)

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

    forecast_stats_df = pd.concat([max_df, p75_df, mean_df, p50_df, p25_df, min_df, high_res_df], axis=1)

    return forecast_stats_df

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

def flood_event_matplotlib(ax, df: pd.DataFrame, *, rp_df: pd.DataFrame = None, plot_titles: list = None, show_maxmin: bool = False):
    """
    Makes the streamflow data and optional metadata into a matplotlib plot

    Args:
        ax: Matplotlib axis object where the figure will be drawn.
        df: the csv response from forecast_stats
        rp_df: the csv response from return_periods
        plot_titles: a list of strings to place in the figure title. each list item will be on a new line.
        show_maxmin: Choose to show or hide the max/min envelope by default
    """

    # Plot other data
    ax.plot(df.index, df.iloc[:,0], label='Observed Flow', color='blue')

    min_date = pd.to_datetime(df.index[0])
    max_date = pd.to_datetime(df.index[-1])

    if rp_df is not None and len(rp_df) > 0:
        y_max = df.iloc[:, 0].max(skipna=True)
        r2 = int(rp_df[2].values[0])
        r5 = int(rp_df[5].values[0])
        r10 = int(rp_df[10].values[0])
        r25 = int(rp_df[25].values[0])
        r50 = int(rp_df[50].values[0])
        r100 = int(rp_df[100].values[0])
        rmax = int(max(r100 + (r100 * 0.05), y_max))

        ax.fill_between([min_date, max_date], [r100 * 0.05, r100 * 0.05], [r100 * 0.05, r100 * 0.05], color=(0 / 255, 0 / 255, 0 / 255, 0), label='Return Periods')
        ax.fill_between([min_date, max_date], [r2, r2], [r5, r5], color=(254 / 255, 240 / 255, 1 / 255, 0.4), label='2 Year: {}'.format(r2), edgecolor=(254 / 255, 240 / 255, 1 / 255))
        ax.fill_between([min_date, max_date], [r5, r5], [r10, r10], color=(253 / 255, 154 / 255, 1 / 255, 0.4), label='5 Year: {}'.format(r5), edgecolor=(253 / 255, 154 / 255, 1 / 255))
        ax.fill_between([min_date, max_date], [r10, r10], [r25, r25], color=(255 / 255, 56 / 255, 5 / 255, 0.4), label='10 Year: {}'.format(r10), edgecolor=(255 / 255, 56 / 255, 5 / 255))
        ax.fill_between([min_date, max_date], [r25, r25], [r50, r50], color=(255 / 255, 0 / 255, 0 / 255, 0.4), label='25 Year: {}'.format(r25), edgecolor=(255 / 255, 0 / 255, 0 / 255))
        ax.fill_between([min_date, max_date], [r50, r50], [r100, r100], color=(128 / 255, 0 / 255, 106 / 255, 0.4), label='50 Year: {}'.format(r50), edgecolor=(128 / 255, 0 / 255, 106 / 255))
        ax.fill_between([min_date, max_date], [r100, r100], [rmax, rmax], color=(128 / 255, 0 / 255, 246 / 255, 0.4), label='100 Year: {}'.format(r100), edgecolor=(128 / 255, 0 / 255, 246 / 255))

    ax.set_xlim(min_date, max_date)
    ax.set_xticks(pd.date_range(start=min_date, end=max_date, periods=5))

    # Set title and labels
    ax.set_title('\n'.join(plot_titles) if plot_titles else 'Observed Streamflow', fontweight='bold')
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


fechas = ['20241010', '20241011', '20241012', '20241013', '20241014', '20241015', '20241016', '20241017', '20241018', '20241019',
          '20241020', '20241021', '20241022', '20241023', '20241024', '20241025', '20241026', '20241027', '20241028', '20241029',
          '20241030', '20241031', '20241101', '20241102', '20241103', '20241104', '20241105', '20241106', '20241107', '20241108',
          '20241109', '20241110']

code = 8112
comid_1 = 12077314
comid_2 = 210265545
lat_GloFAS = 39.275
lon_GloFAS = -1.125
lat_EFAS = 39.258333
lon_EFAS = -1.091667
name = 'Cofrentes at Cabriel River (Spain)'

# Getting Historical (Observed)
observed_historical = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Plot\\{0}\\{1}_HIS.csv".format(name, code), index_col=0)
observed_historical.index = pd.to_datetime(observed_historical.index)
observed_historical.index = observed_historical.index.to_series().dt.strftime("%Y-%m-%d")
observed_historical.index = pd.to_datetime(observed_historical.index)

# Calculating the Return Period
# Version 1
rperiods_obs = return_period_values(observed_historical, comid_1)
rperiods_obs = rperiods_obs.T


# Getting Real Time (Observed)
observed_event = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Plot\\{0}\\{1}_RT.csv".format(name, code), index_col=0)
observed_event.index = pd.to_datetime(observed_event.index)
observed_event.index = observed_event.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
observed_event.index = pd.to_datetime(observed_event.index)


# Getting Historical (Retrospective Simulation)
# Version 1
simulated_v1 = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Retrospective_GEOGLOWS\\{}.csv".format(comid_1), index_col=0)
simulated_v1[simulated_v1 < 0] = 0
simulated_v1.index = pd.to_datetime(simulated_v1.index)
simulated_v1.index = simulated_v1.index.to_series().dt.strftime("%Y-%m-%d")
simulated_v1.index = pd.to_datetime(simulated_v1.index)
simulated_v1 = simulated_v1.loc[simulated_v1.index <= pd.to_datetime("2022-05-31")]

# EFAS
simulated_efas = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Retrospective_EFAS\\{0}_{1}.csv".format(lat_EFAS, lon_EFAS), index_col=0)
simulated_efas[simulated_efas < 0] = 0
simulated_efas.index = pd.to_datetime(simulated_efas.index)
simulated_efas.index = simulated_efas.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
simulated_efas.index = pd.to_datetime(simulated_efas.index)

# GloFAS
simulated_glofas = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Retrospective_GloFAS\\{0}_{1}.csv".format(lat_GloFAS, lon_GloFAS), index_col=0)
simulated_glofas[simulated_glofas < 0] = 0
simulated_glofas.index = pd.to_datetime(simulated_glofas.index)
simulated_glofas.index = simulated_glofas.index.to_series().dt.strftime("%Y-%m-%d")
simulated_glofas.index = pd.to_datetime(simulated_glofas.index)

# Calculating the Return Period
# Version 1
rperiods_v1 = return_period_values(simulated_v1, comid_1)
rperiods_v1 = rperiods_v1.T

# Version 2
simulated_efas_2 = simulated_efas.loc[simulated_efas.index <= pd.to_datetime("2024-10-01")]
rperiods_efas = return_period_values(simulated_efas_2, "{0} {1}".format(lat_EFAS, lon_EFAS))
rperiods_efas = rperiods_efas.T

# GloFAS
simulated_glofas_2 = simulated_glofas.loc[simulated_glofas.index <= pd.to_datetime("2024-10-01")]
rperiods_glofas = return_period_values(simulated_glofas_2, "{0} {1}".format(lat_GloFAS, lon_GloFAS))
rperiods_glofas = rperiods_glofas.T

# Getting Forecast Record
# Version 1
forecast_record_v1 = get_forecast_records_v1(river_id=comid_1, start_date='20241010')
forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
forecast_record_v1.index = forecast_record_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)

# EFAS
forecast_record_efas = get_forecast_records_efas(lat=lat_EFAS, lon=lon_EFAS, start_date='20241010')
forecast_record_efas.index = pd.to_datetime(forecast_record_efas.index)
forecast_record_efas.index = forecast_record_efas.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
forecast_record_efas.index = pd.to_datetime(forecast_record_efas.index)

# GloFAS
forecast_record_glofas = get_forecast_records_glofas(lat=lat_GloFAS, lon=lon_GloFAS, start_date='20241010')
forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)
forecast_record_glofas.index = forecast_record_glofas.index.to_series().dt.strftime("%Y-%m-%d")
forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)

for fecha in fechas:

    print(fecha[0:4], '-', fecha[4:6], '-',  fecha[6:8])

    #Get Forecast Data
    #Version 1
    df1_v1 = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values\\{0}\\row_data\\{1}-{2}-{3}.csv".format(comid_1, fecha[0:4], fecha[4:6], fecha[6:8]), index_col=0)
    df1_v1[df1_v1 < 0] = 0
    df1_v1.index = pd.to_datetime(df1_v1.index)
    df1_v1.index = df1_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    df1_v1.index = pd.to_datetime(df1_v1.index)

    df1_v1_hr = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values\\{0}\\row_data\\{1}-{2}-{3}_HR.csv".format(comid_1, fecha[0:4], fecha[4:6], fecha[6:8]), index_col=0)
    df1_v1_hr[df1_v1_hr < 0] = 0
    df1_v1_hr.index = pd.to_datetime(df1_v1_hr.index)
    df1_v1_hr.index = df1_v1_hr.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    df1_v1_hr.index = pd.to_datetime(df1_v1_hr.index)

    forecast_stats_v1 = forecast_stats_geoglows(ensembles_df=df1_v1, high_res_df=df1_v1_hr)
    forecast_stats_v1 = forecast_stats(df1_v1)

    #EFAS
    df1_efas_00 = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values_EFAS\\{0}-{1}-{2} 00\\{3}_{4}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], lat_EFAS, lon_EFAS), index_col=0)
    df1_efas_00[df1_efas_00 < 0] = 0
    df1_efas_00.index = pd.to_datetime(df1_efas_00.index)
    df1_efas_00.index = df1_efas_00.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    df1_efas_00.index = pd.to_datetime(df1_efas_00.index)
    forecast_stats_efas_00 = forecast_stats(df1_efas_00)

    #GloFAS
    df1_glofas = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Values_GloFAS\\{0}-{1}-{2}\\{3}_{4}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], lat_GloFAS, lon_GloFAS), index_col=0)
    df1_glofas[df1_glofas < 0] = 0
    df1_glofas.index = pd.to_datetime(df1_glofas.index)
    df1_glofas.index = df1_glofas.index.to_series().dt.strftime("%Y-%m-%d")
    df1_glofas.index = pd.to_datetime(df1_glofas.index)
    forecast_stats_glofas = forecast_stats(df1_glofas)

    ###Plot Time Series - Observed Event
    observed_event_plot = observed_event.loc[observed_event.index >= pd.to_datetime(forecast_stats_v1.index[0] - dt.timedelta(days=15))]
    observed_event_plot = observed_event_plot.loc[observed_event_plot.index <= pd.to_datetime(forecast_stats_v1.index[-1])]
    # Definir el rango completo deseado
    fecha_ini = pd.to_datetime(forecast_stats_v1.index[0] - dt.timedelta(days=15))
    fecha_fin = pd.to_datetime(forecast_stats_v1.index[-1])
    rango_fechas = pd.date_range(start=fecha_ini, end=fecha_fin, freq='1H')
    # Reindexar los datos observados a ese rango, rellenando con NaN
    observed_event_plot = (observed_event.reindex(rango_fechas).loc[(rango_fechas >= fecha_ini) & (rango_fechas <= fecha_fin)])

    ###Plot Time Series - Forecast Record
    #Version 1
    try:
        forecast_record_plot_v1 = forecast_record_v1.loc[forecast_record_v1.index >= pd.to_datetime(forecast_stats_v1.index[0] - dt.timedelta(days=15))]
        forecast_record_plot_v1 = forecast_record_plot_v1.loc[forecast_record_plot_v1.index <= pd.to_datetime(forecast_stats_v1.index[0])]
    except Exception as e:
        print(e)

    # Version 2
    forecast_record_plot_efas = forecast_record_efas.loc[forecast_record_efas.index >= pd.to_datetime(forecast_stats_efas_00.index[0] - dt.timedelta(days=15))]
    forecast_record_plot_efas = forecast_record_plot_efas.loc[forecast_record_plot_efas.index <= pd.to_datetime(forecast_stats_efas_00.index[0])]

    # GloFAS
    forecast_record_plot_glofas = forecast_record_glofas.loc[forecast_record_glofas.index >= pd.to_datetime(forecast_stats_glofas.index[0] - dt.timedelta(days=16))]
    forecast_record_plot_glofas = forecast_record_plot_glofas.loc[forecast_record_plot_glofas.index <= pd.to_datetime(forecast_stats_glofas.index[0])]

    ###Plot Time Series - Retrospective
    # Version 1
    simulated_plot_v1 = simulated_v1.loc[simulated_v1.index >= pd.to_datetime(forecast_stats_v1.index[0] - dt.timedelta(days=15))]
    simulated_plot_v1 = simulated_plot_v1.loc[simulated_plot_v1.index <= pd.to_datetime(forecast_stats_v1.index[0])]

    # Version 2
    simulated_plot_efas = simulated_efas.loc[simulated_efas.index >= pd.to_datetime(forecast_stats_efas_00.index[0] - dt.timedelta(days=15))]
    simulated_plot_efas = simulated_plot_efas.loc[simulated_plot_efas.index <= pd.to_datetime(forecast_stats_efas_00.index[0])]

    # GloFAS
    simulated_plot_glofas = simulated_glofas.loc[simulated_glofas.index >= pd.to_datetime(forecast_stats_glofas.index[0] - dt.timedelta(days=16))]
    simulated_plot_glofas = simulated_plot_glofas.loc[simulated_plot_glofas.index <= pd.to_datetime(forecast_stats_glofas.index[0])]

    fig, axs = plt.subplots(4, 1, figsize=(15, 10))
    flood_event_matplotlib(ax=axs[0], df= observed_event_plot, rp_df= rperiods_obs)
    forecast_stats_records_matplotlib(ax=axs[1], df=forecast_stats_v1, rp_df=rperiods_v1, records_df=forecast_record_plot_v1, retro_df=simulated_plot_v1, plot_titles = [f'GEOGLOWS V1 Reach ID: {comid_1}'])
    forecast_stats_records_matplotlib(ax=axs[2], df=forecast_stats_efas_00, rp_df=rperiods_efas, records_df=forecast_record_plot_efas, retro_df=simulated_plot_efas, plot_titles = [f'EFAS Lat-Lon: {lat_EFAS}, {lon_EFAS}'])
    forecast_stats_glofas_plot = forecast_stats_glofas.loc[forecast_stats_glofas.index <= pd.to_datetime(forecast_stats_v1.index[-1])]
    forecast_stats_records_matplotlib(ax=axs[3], df=forecast_stats_glofas_plot, rp_df=rperiods_glofas, records_df=forecast_record_plot_glofas, retro_df=simulated_plot_glofas, plot_titles=[f'GloFAS Lat-Lon: {lat_GloFAS}, {lon_GloFAS}'])
    plt.tight_layout(pad=0.5, h_pad=0.5)
    plt.savefig('G:\\My Drive\\Personal_Files\\Post_Doc\\GEOGLOWS_Applications\\Spain\\Forecast_Plot\\{0}\\{1}.png'.format(name, fecha), dpi=700)
    plt.close(fig)
