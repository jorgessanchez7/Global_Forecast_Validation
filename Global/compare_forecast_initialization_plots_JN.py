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

        try:
            forecast_data = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}.csv".format(yyyy,mm,dd,river_id), index_col=0)
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

        try:
            forecast_data = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}.csv".format(yyyy, mm, dd, river_id), index_col=0)
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

def kge(sim, obs):
    sim, obs = np.array(sim), np.array(obs)
    mask = ~np.isnan(sim) & ~np.isnan(obs)
    sim, obs = sim[mask], obs[mask]
    if len(sim) == 0 or np.std(sim) == 0 or np.std(obs) == 0:
        return np.nan
    r = np.corrcoef(sim, obs)[0, 1]
    alpha = (np.std(sim) / np.mean(sim)) / (np.std(obs) / np.mean(obs))
    beta = np.mean(sim) / np.mean(obs)
    return 1 - np.sqrt((r - 1)**2 + (alpha - 1)**2 + (beta - 1)**2)


def align_and_resample(df_high, df_low, res_low):
    """
    Ajusta df_high (mayor resolución) a la resolución de df_low (res_low).
    """
    if res_low == 'D':  # Diario
        df_high_resampled = df_high.resample('D').mean()
    elif res_low == '3H':  # Cada 3 horas (bloques personalizados)
        df_high_resampled = (df_high.groupby(df_high.index.floor('3H')).mean())
    else:
        df_high_resampled = df_high
    return df_high_resampled

def calc_kge_between(df1, df2, res1, res2):
    """
    Calcula el KGE entre dos series ajustando resoluciones distintas.
    """
    df1c, df2c = df1.copy(), df2.copy()

    # Ajustar resolución si son distintas
    if res1 != res2:
        if res1 == 'H' and res2 == '3H':
            df1c = align_and_resample(df1c, df2c, '3H')
        elif res1 == '3H' and res2 == 'H':
            df2c = align_and_resample(df2c, df1c, '3H')
        elif res1 == 'H' and res2 == 'D':
            df1c = align_and_resample(df1c, df2c, 'D')
        elif res1 == 'D' and res2 == 'H':
            df2c = align_and_resample(df2c, df1c, 'D')
        elif res1 == '3H' and res2 == 'D':
            df1c = align_and_resample(df1c, df2c, 'D')
        elif res1 == 'D' and res2 == '3H':
            df2c = align_and_resample(df2c, df1c, 'D')

    # Alinear índices
    common_idx = df1c.index.intersection(df2c.index)
    if len(common_idx) < 5:
        return np.nan  # insuficientes datos comunes

    df1c, df2c = df1c.loc[common_idx], df2c.loc[common_idx]
    return kge(df1c.iloc[:, 0], df2c.iloc[:, 0])

stations = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Stations_Comparison_v1.csv")
comid_1s = stations['COMID_v1'].to_list()
comid_2s = stations['COMID_v2'].to_list()
latitudes = stations['Lat_GloFAS'].to_list()
longitudes = stations['Lon_GloFAS'].to_list()
names = stations['name'].to_list()

for comid_1, comid_2, name, latitude, longitude in zip(comid_1s, comid_2s, names, latitudes, longitudes):

    print(name, ' - ', comid_1, ' - ', comid_2, ' - ', latitude, ' - ', longitude)

    #Getting Historical (Retrospective Simulation)
    #Version 1
    #era_res_v1 =  requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id=' + str(comid_1) + '&return_format=csv', verify=False).content
    #simulated_v1 = pd.read_csv(io.StringIO(era_res_v1.decode('utf-8')), index_col=0)
    simulated_v1 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GEOGLOWS_v1\\{}.csv".format(comid_1), index_col=0)
    simulated_v1[simulated_v1 < 0] = 0
    simulated_v1.index = pd.to_datetime(simulated_v1.index)
    simulated_v1.index = simulated_v1.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_v1.index = pd.to_datetime(simulated_v1.index)
    simulated_v1 = simulated_v1.loc[simulated_v1.index <= pd.to_datetime("2022-05-31")]
    #simulated_v1.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GEOGLOWS_v1\\{}.csv".format(comid_1))

    #Version 2
    #era_res_v2 = requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivedaily/' + str(comid_2) + '?format=csv', verify=False).content
    #era_res_v2 = requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivehourly/' + str(comid_2) + '?format=csv', verify=False).content
    #simulated_v2 = pd.read_csv(io.StringIO(era_res_v2.decode('utf-8')), index_col=0)
    simulated_v2 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GEOGLOWS_v2\\{}.csv".format(comid_2), index_col=0)
    simulated_v2[simulated_v2 < 0] = 0
    simulated_v2.index = pd.to_datetime(simulated_v2.index)
    #simulated_v2.index = simulated_v2.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_v2.index = simulated_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    simulated_v2.index = pd.to_datetime(simulated_v2.index)
    #simulated_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GEOGLOWS_v2\\{}.csv".format(comid_2))

    #Calculating the Return Period
    #Version 1
    rperiods_v1 = return_period_values(simulated_v1, comid_1)
    rperiods_v1 = rperiods_v1.T
    #rperiods_v1.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Return_Periods\\GEOGLOWS_v1\\{}.csv".format(comid_1))

    #Version 2
    rperiods_v2 = return_period_values(simulated_v2, comid_2)
    rperiods_v2 = rperiods_v2.T
    #rperiods_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Return_Periods\\GEOGLOWS_v2\\{}.csv".format(comid_2))

    #GloFAS
    simulated_glofas = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Retrospective\\GloFAS\\{0}_{1}.csv".format(latitude, longitude), index_col=0)
    simulated_glofas.index = pd.to_datetime(simulated_glofas.index)
    simulated_glofas.index = simulated_glofas.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_glofas.index = pd.to_datetime(simulated_glofas.index)
    rperiods_glofas = return_period_values(simulated_glofas, "{0} {1}".format(latitude, longitude))
    rperiods_glofas = rperiods_glofas.T
    #rperiods_glofas.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Return_Periods\\GloFAS\\{0}_{1}.csv".format(latitude, longitude))

    #Getting Forecast Record
    #Version 1
    forecast_record_v1 = get_forecast_records_v1(comid_1, '20250610')
    #forecast_record_v1 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], comid_1))
    forecast_record_v1[forecast_record_v1 < 0] = 0
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
    forecast_record_v1.index = forecast_record_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
    forecast_record_v1.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v1\\{0}.csv".format(comid_1))

    #Version 2
    forecast_record_v2 = get_forecast_records_v2(comid_2, '20250610')
    #forecast_record_v2 = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], comid_2))
    forecast_record_v2[forecast_record_v2 < 0] = 0
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
    forecast_record_v2.index = forecast_record_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
    forecast_record_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GEOGLOWS_v2\\{0}.csv".format(comid_2))

    # GloFAS
    forecast_record_glofas = get_forecast_records_glofas(latitude, longitude, '20250610')
    #forecast_record_glofas = pd.read_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Values\\{0}-{1}-{2}\\{3}_{4}.csv".format(fecha[0:4], fecha[4:6], fecha[6:8], latitude, longitude))
    forecast_record_glofas[forecast_record_glofas < 0] = 0
    forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)
    forecast_record_glofas.index = forecast_record_glofas.index.to_series().dt.strftime("%Y-%m-%d")
    forecast_record_glofas.index = pd.to_datetime(forecast_record_glofas.index)
    forecast_record_glofas.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\GloFAS\\{0}_{1}.csv".format(latitude, longitude))

    simulated_v1_plot = simulated_v1.loc[simulated_v1.index >= pd.to_datetime(forecast_record_v1.index[0])]
    simulated_v2_plot = simulated_v2.loc[simulated_v2.index >= pd.to_datetime(forecast_record_v2.index[0])]
    simulated_glofas_plot = simulated_glofas.loc[simulated_glofas.index >= pd.to_datetime(forecast_record_glofas.index[0])]

    # Creating the plot
    plt.figure(figsize=(15, 6))

    # Crear figura con dos filas: una para el gráfico, otra para la tabla
    fig, (ax, ax_table) = plt.subplots(
        nrows=2,
        figsize=(15, 7),
        gridspec_kw={'height_ratios': [4, 1]}  # 4 partes para gráfico, 1 para tabla
    )

    # --- Gráfico principal ---
    # (todo lo que tenías antes con plt.plot(...) ahora usa ax.plot(...))
    if not simulated_v1_plot.empty:
        ax.plot(simulated_v1_plot.index, simulated_v1_plot.iloc[:, 0], '#9467bd', label='Retrospective Simulation v1')
    if not simulated_v2_plot.empty:
        ax.plot(simulated_v2_plot.index, simulated_v2_plot.iloc[:, 0], 'blue', label='Retrospective Simulation v2')
    if not simulated_glofas_plot.empty:
        ax.plot(simulated_glofas_plot.index, simulated_glofas_plot.iloc[:, 0], '#1f77b4', label='Retrospective Simulation GloFAS')
    if not forecast_record_v1.empty:
        ax.plot(forecast_record_v1.index, forecast_record_v1['average_flow'], linestyle='--', color='#FFA15A', label='Forecast Record v1')
    if not forecast_record_v2.empty:
        ax.plot(forecast_record_v2.index, forecast_record_v2['average_flow'], linestyle='-', color='#e377c2', label='Forecast Record v2')
    if not forecast_record_glofas.empty:
        ax.plot(forecast_record_glofas.index, forecast_record_glofas['average_flow'], linestyle='-.', color='#d62728', label='Forecast Record GloFAS')

    # Formato del gráfico
    ax.set_title(f'Forecast Initialization Comparison {name}')
    ax.set_xlabel('Date')
    ax.set_ylabel('Streamflow (m³/s)')
    ax.legend()
    ax.grid(True)

    series_dict = {
        'Retrospective Simulation GEOGLOWS v2': (simulated_v2_plot, 'H'),
        'Retrospective Simulation GloFAS': (simulated_glofas_plot, 'D'),
        'Forecast Record GEOGLOWS v1': (forecast_record_v1, '3H'),
        'Forecast Record GEOGLOWS v2': (forecast_record_v2, '3H'),
        'Forecast Record GloFAS': (forecast_record_glofas, 'D')
    }

    names_series = list(series_dict.keys())
    kge_matrix = pd.DataFrame(index=names_series, columns=names_series, dtype=float)

    for i, (name_i, (df_i, res_i)) in enumerate(series_dict.items()):
        for j, (name_j, (df_j, res_j)) in enumerate(series_dict.items()):
            if i == j:
                kge_matrix.loc[name_i, name_j] = calc_kge_between(df_i, df_j, res_i, res_j)
            else:
                kge_matrix.loc[name_i, name_j] = calc_kge_between(df_i, df_j, res_i, res_j)

    kge_matrix = kge_matrix.round(2)

    # --- Crear la tabla en el eje inferior ---
    ax_table.axis('off')  # quitar ejes
    table = ax_table.table(
        cellText=kge_matrix.values,
        rowLabels=kge_matrix.index,
        colLabels=kge_matrix.columns,
        cellLoc='center',
        loc='center'
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1.2, 1.2)

    # Título para la tabla
    ax_table.set_title('KGE Matrix', fontsize=10, fontweight='bold', pad=10)

    plt.tight_layout()
    plt.subplots_adjust(hspace=0.3)  # más espacio entre gráfico y tabla
    plt.savefig(f'G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Initialization_Plots\\Forecast Initialization Comparison {name}.png', dpi=700)
    plt.close(fig)