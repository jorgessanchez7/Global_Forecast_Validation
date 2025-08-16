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

        # Fetch forecast data for the current date
        print(f'{yyyy}-{mm}-{dd} v2')

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

#Stations
comid_1s = [9015333, 9021481, 9031188, 9025707, 9017966, 9075530, 9020865, 9039185, 9023382, 9047884, 8017519, 12061937, 12050781,
           12014938, 5025920, 5031640, 5018188, 600366, 7050298, 13061982, 258721, 13009292, 7054410, 7055650, 947104, 9096566,
           9085273, 9080933, 1018826, 1012389, 10049455, 10009772, 4061319, 13061705, 13059392, 13063849]
comid_2s = [610353609, 670086330, 670081320, 670089457, 620569841, 620953148, 620803818, 621073787, 621030902, 621109666, 470499982,
           220372327, 220720275, 220276772, 441167304, 441185243, 441292437, 220527770, 180234968, 760583077, 540893378, 720111586,
           140759702, 160521695, 770368864, 640458755, 640101447, 630202025, 530187786, 520337491, 280354418, 280758239, 420746965,
           760644642, 760556389, 760541527]
names = ['Guaviare River at Mapiripan (Colombia)', 'Esmeraldas River at Esmeraldas (Ecuador)', 'Guayas River at Duran (Ecuador)',
        'Chone River after joint with Tosagua River (Ecuador)', 'Amazonas_casiquiare_km2764', 'Amazonas_guapore_km3139',
        'Amazonas_negro_km2523', 'Amazonas_tapajos_km0810', 'Amazonas_uaupes_km2380', 'Amazonas_xingu_km1020', 'Balkhash_ili_km0392',
        'Danube_sava_km1327', 'Danube_tisza_km1617', 'Dniepr_dnepr_km1390', 'Ganges-Brahmaputra_brahmaputra_km0809',
        'Ganges-Brahmaputra_ganges_km1381', 'Indus_indus_km0949', 'Kuban_kuban_km0397', 'Lake-Chad_logone_km0500',
        'Mississippi_mississippi_km2378', 'Murray_murray_km0651', 'Nass_nass_km0058', 'Niger_benue_km1000', 'Nile_baro_km4616',
        'Papaloapan_san-Juan_km0134', 'Parana_paraguai_km2622', 'Parana_paraguai_km3506', 'Sao-Francisco_sao-Francisco_km1577',
        'Sepik_sepik_km0150', 'Sungai-Ketapang_sungai-Ketapang_km0121', 'Terek_terek_km0150', 'Volga_kliaz-Ma_km2504',
        'Yangtze_chang-Jiang_km1426', 'North Platte River at Carbon County Wyoming', 'North Platte River at Platte County Wyoming',
         'Big Creek at Carbon County Wyoming']

for comid_1, comid_2, name in zip(comid_1s, comid_2s, names):

    print(name, ' - ', comid_1, ' - ', comid_2)

    #Getting Historical (Retrospective Simulation)
    #Version 1
    #era_res_v1 =  requests.get('https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id=' + str(comid_1) + '&return_format=csv', verify=False).content
    #simulated_v1 = pd.read_csv(io.StringIO(era_res_v1.decode('utf-8')), index_col=0)
    #simulated_v1[simulated_v1 < 0] = 0
    #simulated_v1.index = pd.to_datetime(simulated_v1.index)
    #simulated_v1.index = simulated_v1.index.to_series().dt.strftime("%Y-%m-%d")
    #simulated_v1.index = pd.to_datetime(simulated_v1.index)
    #simulated_v1 = simulated_v1.loc[simulated_v1.index <= pd.to_datetime("2022-05-31")]

    #Version 2
    #era_res_v2 = requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivedaily/' + str(comid_2) + '?format=csv', verify=False).content
    era_res_v2 = requests.get('https://geoglows.ecmwf.int/api/v2/retrospectivehourly/' + str(comid_2) + '?format=csv', verify=False).content
    simulated_v2 = pd.read_csv(io.StringIO(era_res_v2.decode('utf-8')), index_col=0)
    simulated_v2[simulated_v2 < 0] = 0
    simulated_v2.index = pd.to_datetime(simulated_v2.index)
    simulated_v2.index = simulated_v2.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_v2.index = pd.to_datetime(simulated_v2.index)

    #Calculating the Return Period
    #Version 1
    #rperiods_v1 = return_period_values(simulated_v1, comid_1)
    #rperiods_v1 = rperiods_v1.T

    #Version 2
    #rperiods_v2 = return_period_values(simulated_v2, comid_2)
    #rperiods_v2 = rperiods_v2.T

    #Getting Forecast Record
    #Version 1
    forecast_record_v1 = get_forecast_records_v1(comid_1, "20250610")
    forecast_record_v1[forecast_record_v1 < 0] = 0
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
    forecast_record_v1.index = forecast_record_v1.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v1.index = pd.to_datetime(forecast_record_v1.index)
    #forecast_record_v1.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_1))

    #Version 2
    forecast_record_v2 = get_forecast_records_v2(comid_2, "20250610")
    forecast_record_v2[forecast_record_v2 < 0] = 0
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
    forecast_record_v2.index = forecast_record_v2.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    forecast_record_v2.index = pd.to_datetime(forecast_record_v2.index)
    #forecast_record_v2.to_csv("G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Forecast_Record\\{0}.csv".format(comid_2))

    #simulated_v1_plot = simulated_v1.loc[simulated_v1.index >= pd.to_datetime(forecast_record_v1.index[0])]
    simulated_v2_plot = simulated_v2.loc[simulated_v2.index >= pd.to_datetime(forecast_record_v2.index[0])]

    # Creating the plot
    plt.figure(figsize=(15, 6))

    # Retrospective Simulations
    #if not simulated_v1_plot.empty:
    #    plt.plot(simulated_v1_plot.index, simulated_v1_plot.iloc[:, 0], 'b--', label='Retrospective Simulation v1')
    #else:
    #    print("simulated_v1_plot está vacío, no se graficará.")

    if not simulated_v2_plot.empty:
        plt.plot(simulated_v2_plot.index, simulated_v2_plot.iloc[:, 0], 'b-', label='Retrospective Simulation v2')
    else:
        print("simulated_v2_plot está vacío, no se graficará.")

    # Forecast Records
    if not forecast_record_v1.empty:
        plt.plot(forecast_record_v1.index, forecast_record_v1['average_flow'], linestyle='--', color='#FFA15A',
                 label='Forecast Record v1')
    else:
        print("forecast_record_v1 está vacío, no se graficará.")

    if not forecast_record_v2.empty:
        plt.plot(forecast_record_v2.index, forecast_record_v2['average_flow'], linestyle='-', color='#FFA15A',
                 label='Forecast Record v2')
    else:
        print("forecast_record_v2 está vacío, no se graficará.")

    # Formatting
    plt.title('Forecast Initialization Comparison {0}'.format(name))
    plt.xlabel('Date')
    plt.ylabel('Streamflow (m³/s)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    #plt.show()

    #plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Initialization_Plots\\Forecast Initialization Comparison {0}.png'.format(name), dpi=700)
    plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Initialization_Plots_h\\Forecast Initialization Comparison {0} h.png'.format(name), dpi=700)