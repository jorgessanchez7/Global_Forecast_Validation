import math
import numpy as np
import pandas as pd
import datetime as dt
from scipy import interpolate
import matplotlib.pyplot as plt

'''Input Data'''
#Station 1
#Ganges-Brahmaputra_brahmaputra_km0809
obs_input = '409-0388-020-021'
retro_input = '441167304'

# Get Observed Data
observed_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Observed_Hydroweb/{0}.csv'.format(obs_input), index_col=0)
observed_values.index = pd.to_datetime(observed_values.index)
observed_values.index = observed_values.index.to_series().dt.strftime("%Y-%m-%d")
observed_values.index = pd.to_datetime(observed_values.index)

#Get Retrospective Data
retrospective_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Simulated_Data/GEOGLOWS_v2/{0}.csv'.format(retro_input), index_col=0)
retrospective_values.index = pd.to_datetime(retrospective_values.index)
retrospective_values.index = retrospective_values.index.to_series().dt.strftime("%Y-%m-%d")
retrospective_values.index = pd.to_datetime(retrospective_values.index)

# Get Simulated Data
simulated_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/DWLT/GEOGLOWS_v2/{0}-{1}_WL.csv'.format(obs_input, retro_input), index_col=0)
simulated_values.index = pd.to_datetime(simulated_values.index)
simulated_values[simulated_values < 0] = 0
simulated_values.index = simulated_values.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
simulated_values.index = pd.to_datetime(simulated_values.index)

# Get Corrected Data
corrected_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/DWLT/GEOGLOWS_v2/{0}-{1}_WL.csv'.format(obs_input, retro_input), index_col=0)
corrected_values.index = pd.to_datetime(corrected_values.index)
corrected_values[corrected_values < 0] = 0
corrected_values.index = corrected_values.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
corrected_values.index = pd.to_datetime(corrected_values.index)

#Get Forecast Record
#forecast_record = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/forecast_record_streamflow.csv', index_col=0)
forecast_record = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/9009027_forecast_record.csv', index_col=0)
forecast_record.index = pd.to_datetime(forecast_record.index)
forecast_record[forecast_record < 0] = 0
forecast_record.index = forecast_record.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
forecast_record.index = pd.to_datetime(forecast_record.index)

records_df = forecast_record.loc[forecast_record.index >= pd.to_datetime(simulated_values.index[0] - dt.timedelta(days=14))]
records_df = records_df.loc[records_df.index <= pd.to_datetime(simulated_values.index[0])]

#Get Forecast Record Corrected
#corrected_forecast_record = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/corrected_forecast_record_streamflow.csv', index_col=0)
#corrected_forecast_record = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/9009027_corrected_forecast_record.csv', index_col=0)
corrected_forecast_record = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/9009027_corrected_forecast_record_wl.csv', index_col=0)
corrected_forecast_record.index = pd.to_datetime(corrected_forecast_record.index)
corrected_forecast_record[corrected_forecast_record < 0] = 0
corrected_forecast_record.index = corrected_forecast_record.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
corrected_forecast_record.index = pd.to_datetime(corrected_forecast_record.index)

corrected_records_df = corrected_forecast_record.loc[corrected_forecast_record.index >= pd.to_datetime(simulated_values.index[0] - dt.timedelta(days=14))]
corrected_records_df = corrected_records_df.loc[corrected_records_df.index <= pd.to_datetime(simulated_values.index[0])]

#Get Return Periods
rperiods_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/9009027_rperiods_df.csv', index_col=0)

#Get Corrected Return Periods
#corrected_rperiods_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/9009027_corrected_rperiods_df.csv', index_col=0)
corrected_rperiods_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/9009027_corrected_rperiods_df_wl.csv', index_col=0)

# Get Observed FDC
#observed_september = observed_values[observed_values.index.month == 9]
#monObs = observed_september.dropna()
#observed_october = observed_values[observed_values.index.month == 10]
#monObs = observed_october.dropna()
observed_november = observed_values[observed_values.index.month == 11]
monObs = observed_november.dropna()
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
#simulated_september = retrospective_values[retrospective_values.index.month == 9]
#monSim = simulated_september.dropna()
#simulated_october = retrospective_values[retrospective_values.index.month == 10]
#monSim = simulated_october.dropna()
simulated_november = retrospective_values[retrospective_values.index.month == 11]
monSim = simulated_november.dropna()
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

def forecast_stats_records_matplotlib(ax,
                                      df: pd.DataFrame, *,
                                      rp_df: pd.DataFrame = None,
                                      records_df: pd.DataFrame = None,  # Include records_df
                                      plot_titles: list = None,
                                      show_maxmin: bool = False):
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
        r2 = int(rp_df['2'].values[0])
        r5 = int(rp_df['5'].values[0])
        r10 = int(rp_df['10'].values[0])
        r25 = int(rp_df['25'].values[0])
        r50 = int(rp_df['50'].values[0])
        r100 = int(rp_df['100'].values[0])
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

def forecast_stats_records_matplotlib_wl(ax,
                                      df: pd.DataFrame, *,
                                      rp_df: pd.DataFrame = None,
                                      records_df: pd.DataFrame = None,  # Include records_df
                                      plot_titles: list = None,
                                      show_maxmin: bool = False):
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
        r2 = int(rp_df['2'].values[0])
        r5 = int(rp_df['5'].values[0])
        r10 = int(rp_df['10'].values[0])
        r25 = int(rp_df['25'].values[0])
        r50 = int(rp_df['50'].values[0])
        r100 = int(rp_df['100'].values[0])
        rmax = int(max(2 * r100 - r25, y_max))

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
forecast_stats_records_matplotlib(ax=axs[0, 0], df=simulated_values, records_df=records_df)
#forecast_stats_records_matplotlib(ax=axs[0, 0], df=simulated_values, rp_df = rperiods_df, records_df=records_df)

# Plotting the second graph (top-right)
#axs[0, 1].plot(obscdf, obs_bin_edges, label='Obs. FDC')
axs[0, 1].plot(simcdf, sim_bin_edges, label='Sim. FDC', color='#EF553B')
axs[0, 1].set_title('Flow Duration Curve For The Month of November', fontweight='bold')
axs[0, 1].set_ylabel('Streamflow (m³/s)')
axs[0, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[0, 1].legend()
axs[0, 1].grid(True)  # Add grid
axs[0, 1].set_xlim(0, 1)
axs[0, 1].set_xticks(np.arange(0, 1.2, 0.2))

# Plotting the third graph (bottom-left)
forecast_stats_records_matplotlib_wl(ax=axs[1, 0], df=corrected_values, records_df=corrected_records_df)
#forecast_stats_records_matplotlib_wl(ax=axs[1, 0], df=corrected_values, rp_df = corrected_rperiods_df, records_df=corrected_records_df)

# Plotting the fourth graph (bottom-right)
#axs[1, 1].plot(obscdf, obs_bin_edges, label='Obs. FDC')
#axs[1, 1].plot(simcdf, sim_bin_edges, label='Sim. FDC')
axs[1, 1].plot(obscdf, obs_bin_edges, label='Obs. WLDC')
axs[1, 1].set_title('Water Level Duration Curve For The Month of November', fontweight='bold')
axs[1, 1].set_ylabel('Water Level (cm)')
axs[1, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[1, 1].legend()
axs[1, 1].grid(True)  # Add grid
axs[1, 1].set_xlim(0, 1)
axs[1, 1].set_xticks(np.arange(0, 1.2, 0.2))

# Adjusting x-axis dates for upper-left and lower-left plots
for ax in axs.flat[[0, 2]]:
    ax.xaxis.set_major_locator(plt.MaxNLocator(7))  # Adjust the number of ticks as needed

# Setting y-axis range between 0 and 8000 for top plots
axs[0, 0].set_ylim(0, 8000)
axs[0, 1].set_ylim(0, 8000)

# Setting y-axis range between 0 and 7000 for bottom plots
axs[1, 0].set_ylim(0, 700)
axs[1, 1].set_ylim(0, 700)

# Automatically adjust space between plots
plt.tight_layout()

#interpolating functions
f_obs = interpolate.interp1d(obs_bin_edges,obscdf)
f_sim = interpolate.interp1d(sim_bin_edges,simcdf)
#f_cor = interpolate.interp1d(cor_bin_edges,corcdf)

f_obs_inv = interpolate.interp1d(obscdf, obs_bin_edges)
f_sim_inv = interpolate.interp1d(simcdf, sim_bin_edges)
#f_cor_inv = interpolate.interp1d(corcdf, cor_bin_edges)

# Plotting horizontal lines connecting upper-left and upper-right plots
sim_values = simulated_values['flow_75p'].to_frame()
sim_values.dropna(inplace=True)

point1_x = sim_values.index[62]  #point in upper left plot
point1_y = sim_values['flow_75p'][62]  #point in upper left plot
point2_x = f_sim(point1_y)  #point in upper right plot
point2_y = sim_values['flow_75p'][62]  #point in upper right plot
point3_x = point2_x  #point in lower right plot
point3_y = f_obs_inv(point2_x)  #point in lower right plot
point4_x = sim_values.index[62]  #point in lower left plot
point4_y = point3_y  #point in lower left plot

# Plotting horizontal line with markers
axs[0, 0].text(point1_x, point1_y+50, '1', color='black', ha='center', va='bottom')
axs[0, 1].text(point2_x, point2_y+150, '2', color='black', ha='left', va='bottom')
axs[1, 1].text(point3_x, point3_y+20, '3', color='black', ha='right', va='bottom')
axs[1, 0].text(point4_x, point4_y+7, '4', color='black', ha='center', va='bottom')

# Adding points at specified locations
axs[0, 0].scatter(point1_x, point1_y, color='black', s=10)
axs[0, 1].scatter(point2_x, point2_y, color='black', s=10)
axs[1, 1].scatter(point3_x, point3_y, color='black', s=10)
axs[1, 0].scatter(point4_x, point4_y, color='black', s=10)

plt.savefig('/Users/grad/Library/CloudStorage/Box-Box/MSc_Darlly_Rojas/2024_Winter/Updated_Plots/Bias_Correct_Forecast/Forecast Bias Correction Method_WL.png', dpi=700)

# Show the plots
plt.show()