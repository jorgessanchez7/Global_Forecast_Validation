import math
import numpy as np
import pandas as pd
import datetime as dt

from dask.array import corrcoef
from scipy import interpolate
import matplotlib.pyplot as plt

'''Input Data'''

###Station 1###
#Amazonas_casiquiare_km2764
#obs_input = '605-0031-128-005'
#retro_input = '620569841'
#comid_1 = '9017966'
#name = 'Amazonas_casiquiare_km2764'

###Station 2###
#Amazonas_guapore_km3139
#obs_input = '605-0031-223-014'
#retro_input = '620953148'
#comid_1 = '9075530'
#name = 'Amazonas_guapore_km3139'

###Station 3###
#Amazonas_negro_km1739
#obs_input = '605-0031-447-025'
#retro_input = '621010321'
#comid_1 = '9026839'
#name = 'Amazonas_negro_km1739'

###Station 4###
#Amazonas_negro_km2523
#obs_input = '605-0031-447-083'
#retro_input = '620803818'
#comid_1 = '9020865'
#name = 'Amazonas_negro_km2523'

###Station 5###
#Amazonas_tapajos_km0810
#obs_input = '605-0031-586-022'
#retro_input = '621073787'
#comid_1 = '9039185'
#name = 'Amazonas_tapajos_km0810'

###Station 6###
#Amazonas_uaupes_km2380
obs_input = '605-0031-625-001'
retro_input = '621030902'
comid_1 = '9023382'
name = 'Amazonas_uaupes_km2380'

###Station 7###
#Amazonas_xingu_km1020
#obs_input = '604-0031-659-037'
#retro_input = '621109666'
#name = 'Amazonas_xingu_km1020'

###Station 8###
#Balkhash_ili_km0392
#obs_input = '416-0098-003-012'
#retro_input = '470499982'
#name = 'Balkhash_ili_km0392'

###Station 9###
#Congo_kasai_km0868
#obs_input = '109-0267-045-012'
#retro_input = '130721536'
#name = 'Congo_kasai_km0868'

###Station 10###
#Congo_sankuru_km1310
#obs_input = '109-0267-150-002'
#retro_input = '130778089'
#name = 'Congo_sankuru_km1310'

###Station 11###
#Danube_sava_km1327
#obs_input = '205-0297-081-004'
#retro_input = '220372327'
#name = 'Danube_sava_km1327'

###Station 12###
#Danube_tisza_km1617
#obs_input = '205-0297-098-001'
#retro_input = '220720275'
#name = 'Danube_tisza_km1617'

###Station 13###
#Dniepr_dnepr_km1390
#obs_input = '203-0314-005-011'
#retro_input = '220276772'
#name = 'Dniepr_dnepr_km1390'

###Station 13###
#Ganges-Brahmaputra_brahmaputra_km0809
#obs_input = '409-0388-020-021'
#retro_input = '441167304'
#name = 'Ganges-Brahmaputra_brahmaputra_km0809'

###Station 14###
#Ganges-Brahmaputra_ganges_km1381
#obs_input = '409-0388-040-053'
#retro_input = '441185243'
#name = 'Ganges-Brahmaputra_ganges_km1381'

###Station 15###
#Indus_indus_km0949
#obs_input = '412-0492-015-022'
#retro_input = '441292437'
#name = 'Indus_indus_km0949'

###Station 16###
#Indus_indus_km0949
#obs_input = '412-0492-015-022'
#retro_input = '441292437'
#name = 'Indus_indus_km0949'

###Station 17###
#Kuban_kuban_km0397
#obs_input = '204-0604-005-009'
#retro_input = '220527770'
#name = 'Kuban_kuban_km0397'

###Station 18###
#Lake-Chad_logone_km0500
#obs_input = '126-0621-025-003'
#retro_input = '180234968'
#name = 'Lake-Chad_logone_km0500'

###Station 19###
#Mississippi_mississippi_km2378
#obs_input = '714-0763-121-044'
#retro_input = '760583077'
#name = 'Mississippi_mississippi_km2378'

###Station 20###
#Murray_murray_km0651
#obs_input = '508-0795-016-013'
#retro_input = '540893378'
#name = 'Murray_murray_km0651'

###Station 21###
#Nass_nass_km0058
#obs_input = '705-0811-003-004'
#retro_input = '720111586'
#name = 'Nass_nass_km0058'

###Station 22###
#Niger_benue_km1000
#obs_input = '111-0835-008-018'
#retro_input = '140759702'
#name = 'Niger_benue_km1000'

###Station 23###
#Nile_baro_km4616
#obs_input = '122-0837-018-008'
#retro_input = '160521695'
#name = 'Nile_baro_km4616'

###Station 24###
#Papaloapan_san-Juan_km0134
#obs_input = '716-0925-004-002'
#retro_input = '770368864'
#name = 'Papaloapan_san-Juan_km0134'

###Station 25###
#Parana_paraguai_km2622
#obs_input = '609-0934-117-034'
#retro_input = '640458755'
#name = 'Parana_paraguai_km2622'

###Station 26###
#Parana_paraguai_km3506
#obs_input = '609-0934-117-049'
#retro_input = '640101447'
#name = 'Parana_paraguai_km3506'

###Station 27###
#Sao-Francisco_sao-Francisco_km1577
#obs_input = '607-1087-026-035'
#retro_input = '630202025'
#name = 'Sao-Francisco_sao-Francisco_km1577'

###Station 28###
#Sepik_sepik_km0150
#obs_input = '503-1100-006-005'
#retro_input = '530187786'
#name = 'Sepik_sepik_km0150'

###Station 29###
#Sungai-Kapuas_sungai-Kapuas_km0157
#obs_input = '502-1196-006-002'
#retro_input = '520320854'
#name = 'Sungai-Kapuas_sungai-Kapuas_km0157'

###Station 30###
#Sungai-Ketapang_sungai-Ketapang_km0121
#obs_input = '502-1200-001-005'
#retro_input = '520337491'
#name = 'Sungai-Ketapang_sungai-Ketapang_km0121'

###Station 31###
#Terek_terek_km0150
#obs_input = '214-1283-001-001'
#retro_input = '280354418'
#name = 'Terek_terek_km0150'

###Station 32###
#Volga_kliaz-Ma_km2504
#obs_input = '214-1369-065-005'
#retro_input = '280758239'
#name = 'Volga_kliaz-Ma_km2504'

###Station 33###
#Yangtze_chang-Jiang_km1426
#obs_input = '406-1412-008-015'
#retro_input = '420746965'
#name = 'Yangtze_chang-Jiang_km1426'

# Get Observed Data
observed_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Observed_Hydroweb\\{0}.csv'.format(obs_input), index_col=0)
observed_values.index = pd.to_datetime(observed_values.index)
observed_values.index = observed_values.index.to_series().dt.strftime("%Y-%m-%d")
observed_values.index = pd.to_datetime(observed_values.index)

#Get Retrospective Data
retrospective_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Simulated_Data\\GEOGLOWS_v1\\{0}.csv'.format(comid_1), index_col=0)
retrospective_values.index = pd.to_datetime(retrospective_values.index)
retrospective_values.index = retrospective_values.index.to_series().dt.strftime("%Y-%m-%d")
retrospective_values.index = pd.to_datetime(retrospective_values.index)

# Get Simulated Data
simulated_values = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v1_test\\Forecast_Stats\\2025-05-15\\{0}.csv".format(comid_1), index_col=0)
simulated_values.index = pd.to_datetime(simulated_values.index)
simulated_values[simulated_values < 0] = 0
simulated_values.index = simulated_values.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
simulated_values.index = pd.to_datetime(simulated_values.index)

# Get Corrected Data
corrected_values = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v1_test\\Forecast_Stats\\2025-05-15\\{0}_WL.csv".format(comid_1), index_col=0)
corrected_values.index = pd.to_datetime(corrected_values.index)
corrected_values[corrected_values < 0] = 0
corrected_values.index = corrected_values.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
corrected_values.index = pd.to_datetime(corrected_values.index)

#Get Forecast Record
forecast_record = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v1_test\\Forecast_Record\\{0}.csv".format(comid_1), index_col=0)
forecast_record.index = pd.to_datetime(forecast_record.index)
forecast_record[forecast_record < 0] = 0
forecast_record.index = forecast_record.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
forecast_record.index = pd.to_datetime(forecast_record.index)

records_df = forecast_record.loc[forecast_record.index >= pd.to_datetime(simulated_values.index[0] - dt.timedelta(days=10))]
records_df = records_df.loc[records_df.index <= pd.to_datetime(simulated_values.index[0])]

#Get Forecast Record Corrected
corrected_forecast_record = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v1_test\\Forecast_Record\\{0}_WL.csv".format(comid_1), index_col=0)
corrected_forecast_record.index = pd.to_datetime(corrected_forecast_record.index)
corrected_forecast_record[corrected_forecast_record < 0] = 0
corrected_forecast_record.index = corrected_forecast_record.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
corrected_forecast_record.index = pd.to_datetime(corrected_forecast_record.index)

corrected_records_df = corrected_forecast_record.loc[corrected_forecast_record.index >= pd.to_datetime(simulated_values.index[0] - dt.timedelta(days=10))]
corrected_records_df = corrected_records_df.loc[corrected_records_df.index <= pd.to_datetime(simulated_values.index[0])]

#Get Return Periods
rperiods_df = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v1_test\\Return_Periods\\{}.csv".format(comid_1), index_col=0)
rperiods_df = rperiods_df.T

#Get Corrected Return Periods
corrected_rperiods_df = pd.read_csv("G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\GEOGLOWS_v1_test\\Return_Periods\\{}_WL.csv".format(comid_1), index_col=0)
corrected_rperiods_df = corrected_rperiods_df.T

# Get Observed FDC
observed_may = observed_values[observed_values.index.month == 5]
monObs = observed_may.dropna()
obs_tempMax = np.max(monObs.max())
obs_tempMin = np.min(monObs.min())
obs_maxVal = math.ceil(obs_tempMax)
obs_minVal = math.floor(obs_tempMin)
n_elementos_obs = len(monObs.iloc[:, 0].values)
n_marcas_clase_obs = math.ceil(1+(3.322*math.log10(n_elementos_obs)))
step_obs = (obs_maxVal-obs_minVal)/n_marcas_clase_obs
bins_obs = np.arange(-np.min(step_obs), obs_maxVal + 2 * np.min(step_obs), np.min(step_obs))
if (bins_obs[0] == 0):
    bins_obs = np.concatenate((-bins_obs[1], bins_obs))
elif (bins_obs[0] > 0):
    bins_obs = np.concatenate((-bins_obs[0], bins_obs))
obs_counts, bin_edges_obs = np.histogram(monObs, bins=bins_obs)
obs_bin_edges = bin_edges_obs[1:]
obs_counts = obs_counts.astype(float) / monObs.size
obscdf = np.cumsum(obs_counts)

# Get Simulated FDC
simulated_may = retrospective_values[retrospective_values.index.month == 5]
monSim = simulated_may.dropna()
sim_tempMax = np.max(monSim.max())
sim_tempMin = np.min(monSim.min())
sim_maxVal = math.ceil(sim_tempMax)
sim_minVal = math.floor(sim_tempMin)
n_elementos_sim = len(monSim.iloc[:, 0].values)
n_marcas_clase_sim = math.ceil(1+(3.322*math.log10(n_elementos_sim)))
step_sim = (sim_maxVal-sim_minVal)/n_marcas_clase_sim
bins_sim = np.arange(-np.min(step_sim), sim_maxVal + 2 * np.min(step_sim), np.min(step_sim))
if (bins_sim[0] == 0):
    bins_sim = np.concatenate((-bins_sim[1], bins_sim))
elif (bins_sim[0] > 0):
    bins_sim = np.concatenate((-bins_sim[0], bins_sim))
sim_counts, bin_edges_sim = np.histogram(monSim, bins=bins_sim)
sim_bin_edges = bin_edges_sim[1:]
sim_counts = sim_counts.astype(float) / monSim.size
simcdf = np.cumsum(sim_counts)

return_periods = [2, 5, 10, 25, 50, 100]

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
    legend = ax.legend(loc='upper left')
    for text in legend.get_texts():
        if text.get_text() == 'Return Periods':
            text.set_fontweight('bold')
        if rp_df is not None and len(rp_df) > 0:
            if '2 Year: {}'.format(r2) in text.get_text():
                text.set_fontsize(7)  # Set font size for '2 Year'
            if '5 Year: {}'.format(r5) in text.get_text():
                text.set_fontsize(7)  # Set font size for '5 Year'
            if '10 Year: {}'.format(r10) in text.get_text():
                text.set_fontsize(7)  # Set font size for '10 Year'
            if '25 Year: {}'.format(r25) in text.get_text():
                text.set_fontsize(7)  # Set font size for '25 Year'
            if '50 Year: {}'.format(r50) in text.get_text():
                text.set_fontsize(7)  # Set font size for '50 Year'
            if '100 Year: {}'.format(r100) in text.get_text():
                text.set_fontsize(7)  # Set font size for '100 Year'

def forecast_stats_records_matplotlib_wl(ax, df: pd.DataFrame, *, rp_df: pd.DataFrame = None, records_df: pd.DataFrame = None, plot_titles: list = None, show_maxmin: bool = False):
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
    #    ax.fill_between(dates_stats, plot_data['flow_max'], plot_data['flow_min'], color='lightblue', label='Maximum & Minimum')

    ax.fill_between(dates_stats, plot_data['flow_max'], plot_data['flow_min'], color='lightblue',label='Maximum & Minimum', edgecolor='black', linestyle='--')

    # Plot percentiles
    ax.fill_between(dates_stats, plot_data['flow_75%'], plot_data['flow_25%'], color='lightgreen',label='25-75 Percentile', edgecolor='green')

    # Plot other data
    ax.plot(dates_stats, plot_data['flow_avg'], label='Average Water Level', color='blue')
    ax.plot(dates_stats, plot_data['flow_med'], label='Median Water Level', color='red')
    ax.plot(dates_high_res, plot_data['high_res'], label='High-Resolution Forecast', color='black')

    # Plot records_df if provided
    if records_df is not None and len(records_df) > 0:
        ax.plot(records_df.index, records_df.iloc[:, 0].values, label='1st day Forecast', color='#FFA15A')

    if rp_df is not None and len(rp_df) > 0:
        y_max = max(df['flow_max'])
        r2 = round(rp_df[2].values[0], 2)
        r5 = round(rp_df[5].values[0], 2)
        r10 = round(rp_df[10].values[0], 2)
        r25 = round(rp_df[25].values[0], 2)
        r50 = round(rp_df[50].values[0], 2)
        r100 = round(rp_df[100].values[0], 2)
        rmax = round(max(r100 + (r100 * 0.0025), y_max), 2)

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
    ax.set_title('\n'.join(plot_titles) if plot_titles else 'Forecasted Water Level', fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Water Level (cm)')

    # Rotate date labels for better readability
    plt.xticks(rotation=0)

    # Add grid
    ax.grid(True, linestyle='--', alpha=0.7)  # Add grid with dashed lines and transparency

    # Create the legend and set the font weight to bold
    legend = ax.legend(loc='upper left')
    for text in legend.get_texts():
        if text.get_text() == 'Return Periods':
            text.set_fontweight('bold')
        if rp_df is not None and len(rp_df) > 0:
            if '2 Year: {}'.format(r2) in text.get_text():
                text.set_fontsize(7)  # Set font size for '2 Year'
            if '5 Year: {}'.format(r5) in text.get_text():
                text.set_fontsize(7)  # Set font size for '5 Year'
            if '10 Year: {}'.format(r10) in text.get_text():
                text.set_fontsize(7)  # Set font size for '10 Year'
            if '25 Year: {}'.format(r25) in text.get_text():
                text.set_fontsize(7)  # Set font size for '25 Year'
            if '50 Year: {}'.format(r50) in text.get_text():
                text.set_fontsize(7)  # Set font size for '50 Year'
            if '100 Year: {}'.format(r100) in text.get_text():
                text.set_fontsize(7)  # Set font size for '100 Year'

# Creating a 2x2 grid of subplots
fig, axs = plt.subplots(2, 2, figsize=(15, 10))

# Plotting the first graph (top-left)
###Station 1###
#forecast_stats_records_matplotlib(ax=axs[0, 0], df=simulated_values, rp_df = rperiods_df, records_df=records_df)

###Station 3###
#forecast_stats_records_matplotlib(ax=axs[0, 0], df=simulated_values, rp_df = rperiods_df, records_df=records_df)

###Station 4###
#forecast_stats_records_matplotlib(ax=axs[0, 0], df=simulated_values, rp_df = rperiods_df, records_df=records_df)

###Station 5###
#forecast_stats_records_matplotlib(ax=axs[0, 0], df=simulated_values, records_df=records_df)

###Station 6###
forecast_stats_records_matplotlib(ax=axs[0, 0], df=simulated_values, rp_df = rperiods_df, records_df=records_df)

# Plotting the second graph (top-right)
#axs[0, 1].plot(obscdf, obs_bin_edges, label='Obs. FDC')
axs[0, 1].plot(simcdf, sim_bin_edges, label='Sim. FDC', color='#EF553B')
axs[0, 1].set_title('Flow Duration Curve For The Month of May', fontweight='bold')
axs[0, 1].set_ylabel('Streamflow (m³/s)')
axs[0, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[0, 1].legend()
axs[0, 1].grid(True)  # Add grid
axs[0, 1].set_xlim(0, 1)
axs[0, 1].set_xticks(np.arange(0, 1.2, 0.2))

# Plotting the third graph (bottom-left)
###Station 1###
#forecast_stats_records_matplotlib_wl(ax=axs[1, 0], df=corrected_values, rp_df = corrected_rperiods_df, records_df=corrected_records_df)

###Station 3###
#forecast_stats_records_matplotlib_wl(ax=axs[1, 0], df=corrected_values, rp_df = corrected_rperiods_df, records_df=corrected_records_df)

###Station 4###
#forecast_stats_records_matplotlib_wl(ax=axs[1, 0], df=corrected_values, rp_df = corrected_rperiods_df, records_df=corrected_records_df)

###Station 5###
#forecast_stats_records_matplotlib_wl(ax=axs[1, 0], df=corrected_values, records_df=corrected_records_df)

###Station 6###
forecast_stats_records_matplotlib_wl(ax=axs[1, 0], df=corrected_values, rp_df = corrected_rperiods_df, records_df=corrected_records_df)


# Plotting the fourth graph (bottom-right)
#axs[1, 1].plot(obscdf, obs_bin_edges, label='Obs. FDC')
#axs[1, 1].plot(simcdf, sim_bin_edges, label='Sim. FDC')
axs[1, 1].plot(obscdf, obs_bin_edges, label='Obs. WLDC')
axs[1, 1].set_title('Water Level Duration Curve For The Month of May', fontweight='bold')
axs[1, 1].set_ylabel('Water Level (cm)')
axs[1, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[1, 1].legend()
axs[1, 1].grid(True)  # Add grid
axs[1, 1].set_xlim(0, 1)
axs[1, 1].set_xticks(np.arange(0, 1.2, 0.2))

# Adjusting x-axis dates for upper-left and lower-left plots
for ax in axs.flat[[0, 2]]:
    ax.xaxis.set_major_locator(plt.MaxNLocator(7))  # Adjust the number of ticks as needed

###Station 1###
# Setting y-axis range between 0 and 8000 for top plots
#axs[0, 0].set_ylim(0, 12500)
#axs[0, 1].set_ylim(0, 12500)
# Setting y-axis range between 0 and 7000 for bottom plots
#axs[1, 0].set_ylim(81, 89.5)
#axs[1, 1].set_ylim(81, 89.5)

###Station 3###
# Setting y-axis range between 0 and 8000 for top plots
#axs[0, 0].set_ylim(0, 200)
#axs[0, 1].set_ylim(0, 200)
# Setting y-axis range between 0 and 7000 for bottom plots
#axs[1, 0].set_ylim(19, 27)
#axs[1, 1].set_ylim(19, 27)

###Station 4###
# Setting y-axis range between 0 and 8000 for top plots
#axs[0, 0].set_ylim(0, 30000)
#axs[0, 1].set_ylim(0, 30000)
# Setting y-axis range between 0 and 7000 for bottom plots
#axs[1, 0].set_ylim(72, 80.3)
#axs[1, 1].set_ylim(72, 80.3)

###Station 5###
# Setting y-axis range between 0 and 8000 for top plots
#axs[0, 0].set_ylim(0, 50000)
#axs[0, 1].set_ylim(0, 50000)
# Setting y-axis range between 0 and 7000 for bottom plots
#axs[1, 0].set_ylim(3.5, 14.5)
#axs[1, 1].set_ylim(3.5, 14.5)

###Station 6###
# Setting y-axis range between 0 and 8000 for top plots
axs[0, 0].set_ylim(0, 19500)
axs[0, 1].set_ylim(0, 19500)
# Setting y-axis range between 0 and 7000 for bottom plots
axs[1, 0].set_ylim(64, 71.5)
axs[1, 1].set_ylim(64, 71.5)

# Automatically adjust space between plots
plt.tight_layout()

#interpolating functions
f_obs = interpolate.interp1d(obs_bin_edges,obscdf)
f_sim = interpolate.interp1d(sim_bin_edges,simcdf)
#f_cor = interpolate.interp1d(cor_bin_edges,corcdf)

f_obs_inv = interpolate.interp1d(obscdf, obs_bin_edges)
f_sim_inv = interpolate.interp1d(simcdf, sim_bin_edges)
#f_cor_inv = interpolate.interp1d(corcdf, cor_bin_edges)

###Station 1###
# Plotting horizontal lines connecting upper-left and upper-right plots
#sim_values = simulated_values['flow_max'].to_frame()
#sim_values.dropna(inplace=True)

###Station 3###
# Plotting horizontal lines connecting upper-left and upper-right plots
#sim_values = simulated_values['flow_max'].to_frame()
#sim_values.dropna(inplace=True)

###Station 4###
# Plotting horizontal lines connecting upper-left and upper-right plots
#sim_values = simulated_values['flow_75p'].to_frame()
#sim_values.dropna(inplace=True)

###Station 5###
# Plotting horizontal lines connecting upper-left and upper-right plots
#sim_values = simulated_values['flow_max'].to_frame()
#sim_values.dropna(inplace=True)

###Station 6###
# Plotting horizontal lines connecting upper-left and upper-right plots
sim_values = simulated_values['flow_75p'].to_frame()
sim_values.dropna(inplace=True)

###Station 1###
#point1_x = sim_values.index[63]  #point in upper left plot
#point1_y = sim_values['flow_max'][63]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = sim_values['flow_max'][63]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = f_obs_inv(point2_x)  #point in lower right plot
#point4_x = sim_values.index[63]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

###Station 3###
#point1_x = sim_values.index[63]  #point in upper left plot
#point1_y = sim_values['flow_max'][63]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = sim_values['flow_max'][63]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = f_obs_inv(point2_x)  #point in lower right plot
#point4_x = sim_values.index[63]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

###Station 4###
#point1_x = sim_values.index[62]  #point in upper left plot
#point1_y = sim_values['flow_75p'][62]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = sim_values['flow_75p'][62]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = f_obs_inv(point2_x)  #point in lower right plot
#point4_x = sim_values.index[62]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

###Station 5###
#point1_x = sim_values.index[67]  #point in upper left plot
#point1_y = sim_values['flow_max'][67]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = sim_values['flow_max'][67]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = f_obs_inv(point2_x)  #point in lower right plot
#point4_x = sim_values.index[67]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

###Station 6###
point1_x = sim_values.index[70]  #point in upper left plot
point1_y = sim_values['flow_75p'][70]  #point in upper left plot
point2_x = f_sim(point1_y)  #point in upper right plot
point2_y = sim_values['flow_75p'][70]  #point in upper right plot
point3_x = point2_x  #point in lower right plot
point3_y = f_obs_inv(point2_x)  #point in lower right plot
point4_x = sim_values.index[70]  #point in lower left plot
point4_y = point3_y  #point in lower left plot

# Plotting horizontal line with markers
axs[0, 0].text(point1_x, point1_y+50, '1', color='black', ha='center', va='bottom')
axs[0, 1].text(point2_x, point2_y+150, '2', color='black', ha='left', va='bottom')
#axs[0, 0].text(point1_x, point1_y+5, '1', color='black', ha='center', va='bottom') #Station 3
#axs[0, 1].text(point2_x, point2_y+15, '2', color='black', ha='left', va='bottom') #Station 3
axs[1, 1].text(point3_x, point3_y+0.20, '3', color='black', ha='right', va='bottom')
axs[1, 0].text(point4_x, point4_y+0.15, '4', color='black', ha='center', va='bottom')

# Adding points at specified locations
axs[0, 0].scatter(point1_x, point1_y, color='black', s=10)
axs[0, 1].scatter(point2_x, point2_y, color='black', s=10)
axs[1, 1].scatter(point3_x, point3_y, color='black', s=10)
axs[1, 0].scatter(point4_x, point4_y, color='black', s=10)

plt.savefig('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Plots\\Forecast DWLT Method {} May_v2.png'.format(name), dpi=700)

# Show the plots
plt.show()

print(rperiods_df)
print(corrected_rperiods_df)