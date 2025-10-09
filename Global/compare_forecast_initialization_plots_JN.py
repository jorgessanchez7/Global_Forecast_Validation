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

#Stations
comid_1s = [9015333, 9009049, 9017966, 9075530, 9020865, 9023382, 9039185, 9047884, 8017519, 13063849, 9007779, 9010517, 9025707, 12061937,
            12050781, 12014938, 9011402, 9011932, 9008503, 9021481, 5025920, 5031640, 9009403, 9031188, 5018188, 600366, 9010683, 7050298,
            9012628, 13061982, 258721, 13009292, 7054410, 7055650, 13061705, 13059392, 947104, 9096566, 9085273, 9008223, 9010077, 9011280,
            9005322, 9008234, 9080933, 1018826, 1012389, 10049455, 9009300, 10009772, 4061319]
comid_2s = [610353609, 610439693, 620569841, 620953148, 620803818, 621030902, 621073787, 621109666, 470499982, 760541527, 610382462,
            610421255, 670089457, 220372327, 220720275, 220276772, 610408076, 610384326, 610392160, 670086330, 441167304, 441185243,
            610327062, 670081320, 441292437, 220527770, 610350855, 180234968, 610372901, 760583077, 540893378, 720111586, 140759702,
            160521695, 760644642, 760556389, 770368864, 640458755, 640101447, 610420314, 610436201, 610453830, 610358623, 610411513,
            630202025, 530187786, 520337491, 280354418, 610368409, 280758239, 420746965]
latitudes = [2.875, 5.475, 1.925, -12.625, 0.925, 0.025, -4.225, -6.125, 44.675, 41.175, 5.975, 4.775, -0.625, 44.725, 47.175, 53.175,
             4.425, 4.225, 5.675, 0.875, 26.775, 25.575, 5.225, -2.175, 28.575, 45.375, 4.775, 10.175, 4.125, 41.775, -34.125, 55.275,
             8.675, 8.175, 41.875, 42.575, 18.175, -19.025, -15.475, 5.775, 4.975, 4.475, 7.275, 5.775, -14.175, -4.175, -1.575, 43.475,
             5.525, 56.475, 30.375]
longitudes = [-72.125, -76.575, -66.625, -63.425, -67.025, -67.275, -55.875, -52.525, 76.475, -106.525, -75.825, -75.925, -80.325, 19.775,
              20.275, 30.275, -75.875, -74.625, -73.225, -79.625, 93.475, 81.625, -75.825, -79.875, 70.025, 40.275, -75.625, 15.475, -76.225,
              -90.275, 141.325, -129.075, 10.275, 34.875, -107.025, -105.025, -95.375, -57.475, -57.375, -72.875, -75.875, -74.625, -75.425,
              -73.025, -43.675, 143.775, 110.425, 46.375, -72.775, 41.975, 111.675]
names = ['Guaviare River at Mapiripan (Colombia)', 'AGUASAL [11017010]', 'Amazonas_casiquiare_km2764', 'Amazonas_guapore_km3139',
         'Amazonas_negro_km2523', 'Amazonas_uaupes_km2380', 'Amazonas_tapajos_km0810', 'Amazonas_xingu_km1020', 'Balkhash_ili_km0392',
         'Big Creek at Carbon County Wyoming', 'BOLOMBOLO', 'CARTAGO [26127040]', 'Chone River after joint with Tosagua River (Ecuador)',
         'Danube_sava_km1327', 'Danube_tisza_km1617', 'Dniepr_dnepr_km1390', 'EL ALAMBRADO', 'EL LIMONAR [21197150]', 'EL PALO [24037030]',
         'Esmeraldas River at Esmeraldas (Ecuador)', 'Ganges-Brahmaputra_brahmaputra_km0809', 'Ganges-Brahmaputra_ganges_km1381',
         'GUARACAS [26147080]', 'Guayas River at Duran (Ecuador)', 'Indus_indus_km0949', 'Kuban_kuban_km0397', 'LA BANANERA 6-909',
         'Lake-Chad_logone_km0500', 'MATEGUADUA [26107130]', 'Mississippi_mississippi_km2378', 'Murray_murray_km0651', 'Nass_nass_km0058',
         'Niger_benue_km1000', 'Nile_baro_km4616', 'North Platte River at Carbon County Wyoming', 'North Platte River at Platte County Wyoming',
         'Papaloapan_san-Juan_km0134', 'Parana_paraguai_km2622', 'Parana_paraguai_km3506', 'PUENTE CHAMEZA [24037290]', 'PUENTE NEGRO [26147140]',
         'PUENTE PORTILLO [21207960]', 'PUERTO VALDIVIA', 'SAN RAFAEL [24037190]', 'Sao-Francisco_sao-Francisco_km1577', 'Sepik_sepik_km0150',
         'Sungai-Ketapang_sungai-Ketapang_km0121', 'Terek_terek_km0150', 'VADO HONDO [35197020]', 'Volga_kliaz-Ma_km2504', 'Yangtze_chang-Jiang_km1426']

fechas = ['20250610', '20250611', '20250612', '20250613', '20250614', '20250615', '20250616', '20250617', '20250618', '20250619',
          '20250620', '20250621', '20250622', '20250623', '20250624', '20250625', '20250626', '20250627', '20250628', '20250629',
          '20250630', '20250701', '20250702', '20250703', '20250704', '20250705', '20250706', '20250707', '20250708', '20250709',
          '20250710', '20250711', '20250712', '20250713', '20250714', '20250715', '20250716', '20250717', '20250718', '20250719',
          '20250720', '20250721', '20250722', '20250723', '20250724', '20250725', '20250726', '20250727', '20250728', '20250729',
          '20250730', '20250731', '20250801', '20250802', '20250803', '20250804', '20250805', '20250806', '20250807', '20250808',
          '20250809', '20250810', '20250811', '20250812', '20250813', '20250814', '20250815', '20250816', '20250817', '20250818',
          '20250819', '20250820', '20250821', '20250822', '20250823', '20250824', '20250825', '20250826', '20250827', '20250828',
          '20250829', '20250830', '20250831', '20250901', '20250902', '20250903', '20250904', '20250905', '20250906', '20250907',
          '20250908', '20250909', '20250910', '20250911', '20250912', '20250913', '20250914', '20250915', '20250916', '20250917',
          '20250918', '20250919', '20250920', '20250921', '20250922', '20250923', '20250924', '20250925', '20250926', '20250927',
          '20250928', '20250929', '20250930', '20251001', '20251002', '20251003', '20251004', '20251005', '20251006', '20251007',
          '20251008']

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

    # Retrospective Simulations
    if not simulated_v1_plot.empty:
        plt.plot(simulated_v1_plot.index, simulated_v1_plot.iloc[:, 0], 'b--', label='Retrospective Simulation v1')
    else:
        print("simulated_v1_plot está vacío, no se graficará.")

    if not simulated_v2_plot.empty:
        plt.plot(simulated_v2_plot.index, simulated_v2_plot.iloc[:, 0], 'b-', label='Retrospective Simulation v2')
    else:
        print("simulated_v2_plot está vacío, no se graficará.")

    if not simulated_glofas_plot.empty:
        plt.plot(simulated_glofas_plot.index, simulated_glofas_plot.iloc[:, 0], 'b-.', label='Retrospective Simulation GloFAS')
    else:
        print("simulated_glofas_plot está vacío, no se graficará.")

    # Forecast Records
    if not forecast_record_v1.empty:
        plt.plot(forecast_record_v1.index, forecast_record_v1['average_flow'], linestyle='--', color='#FFA15A', label='Forecast Record v1')
    else:
        print("forecast_record_v1 está vacío, no se graficará.")

    if not forecast_record_v2.empty:
        plt.plot(forecast_record_v2.index, forecast_record_v2['average_flow'], linestyle='-', color='#FFA15A', label='Forecast Record v2')
    else:
        print("forecast_record_v2 está vacío, no se graficará.")

    if not forecast_record_glofas.empty:
        plt.plot(forecast_record_glofas.index, forecast_record_glofas['average_flow'], linestyle='-.', color='#FFA15A', label='Forecast Record GloFAS')
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

    plt.savefig('G:\\My Drive\\GEOGLOWS\\Forecast_Comparison\\Initialization_Plots_h\\Forecast Initialization Comparison {0}.png'.format(name), dpi=700)