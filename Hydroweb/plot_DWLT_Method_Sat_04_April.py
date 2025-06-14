import math
import numpy as np
import pandas as pd
import datetime as dt
import hydrostats.data
from scipy import interpolate
import matplotlib.pyplot as plt

'''Input Data'''
###Station 1###
obs_input = '409-0388-020-021'
retro_input = '441167304'
name = 'Ganges-Brahmaputra_brahmaputra_km0809'

###Station 2###
#obs_input = '203-0314-005-011'
#retro_input = '220276772'
#name = 'Dniepr_dnepr_km1390'

###Station 3###
#obs_input = '609-0934-117-049'
#retro_input = '640101447'
#name = 'Parana_paraguai_km3506'

###Station 4###
#obs_input = '204-0604-005-009'
#retro_input = '220527770'
#name = 'Kuban_kuban_km0397'

###Station 5###
#obs_input = '607-1087-026-035'
#retro_input = '630202025'
#name = 'Sao-Francisco_sao-Francisco_km1577'


# Get Observed Data
#observed_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Observed_Hydroweb/{0}.csv'.format(obs_input), index_col=0)
observed_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Observed_Hydroweb\\{0}.csv'.format(obs_input), index_col=0)
observed_values.index = pd.to_datetime(observed_values.index)
observed_april = observed_values[observed_values.index.month == 4]

dayavg_obs_april = hydrostats.data.daily_average(observed_april, rolling=True)

# Get Simulated Data
#simulated_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Simulated_Data/GEOGLOWS_v2/{0}.csv'.format(retro_input), index_col=0)
simulated_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Simulated_Data\\GEOGLOWS_v2\\{0}.csv'.format(retro_input), index_col=0)
simulated_values.index = pd.to_datetime(simulated_values.index)
simulated_april = simulated_values[simulated_values.index.month == 4]
simulated_april_2 = simulated_april.loc[simulated_april.index >= pd.to_datetime(dt.datetime(observed_april.index[0].year, observed_april.index[0].month, 1))]

dayavg_sim_april = hydrostats.data.daily_average(simulated_april_2, rolling=True)

# Get Corrected Data
#corrected_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/DWLT/GEOGLOWS_v2/{0}-{1}_WL.csv'.format(obs_input, retro_input), index_col=0)
corrected_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\DWLT\\GEOGLOWS_v2\\{0}-{1}_WL.csv'.format(obs_input, retro_input), index_col=0)
corrected_values.index = pd.to_datetime(corrected_values.index)
corrected_april = corrected_values[corrected_values.index.month == 4]
corrected_april_2 = corrected_april.loc[corrected_april.index >= pd.to_datetime(dt.datetime(observed_april.index[0].year, observed_april.index[0].month, 1))]

dayavg_corr_april = hydrostats.data.daily_average(corrected_april_2, rolling=True)

# Get Observed FDC
monObs = observed_april.dropna()
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
monSim = simulated_april.dropna()
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

# Get Corrected FDC
monCor = corrected_april.dropna()
cor_tempMax = np.max(monCor.max())
cor_tempMin = np.min(monCor.min())
cor_maxVal = math.ceil(cor_tempMax)
cor_minVal = math.floor(cor_tempMin)
n_elementos_cor = len(monCor.iloc[:, 0].values)
n_marcas_clase_cor = math.ceil(1+(3.322*math.log10(n_elementos_sim)))
step_cor = (cor_maxVal-cor_minVal)/n_marcas_clase_cor
bins_cor = np.arange(-np.min(step_cor), cor_maxVal + 2 * np.min(step_cor), np.min(step_cor))
if (bins_cor[0] == 0):
    bins_cor = np.concatenate((-bins_cor[1], bins_cor))
elif (bins_cor[0] > 0):
    bins_cor = np.concatenate((-bins_cor[0], bins_cor))
cor_counts, bin_edges_cor = np.histogram(monCor, bins=bins_cor)
cor_bin_edges = bin_edges_cor[1:]
cor_counts = cor_counts.astype(float) / monCor.size
corcdf = np.cumsum(cor_counts)

# Creating a 2x2 grid of subplots
fig, axs = plt.subplots(2, 2, figsize=(15, 10))

#Reindexing observed data
date_range = pd.date_range(start=simulated_april_2.index.min(), end=simulated_april_2.index.max(), freq='D')
observed_april_2 = observed_april.reindex(date_range)
observed_april_2.index.name = 'Datetime'
observed_april_2 = observed_april_2[observed_april_2.index.month == 4]

# Usamos directamente el índice actual del dataframe, sin importar qué fechas son
plot_index = range(1, len(observed_april_2) + 1)

# Asignar índice artificial a cada DataFrame (todos tienen igual longitud tras reindexado)

observed_april_2 = observed_april_2.copy()
observed_april_2["plot_index"] = plot_index

simulated_april_2 = simulated_april_2.copy()
simulated_april_2["plot_index"] = plot_index

corrected_april_2 = corrected_april_2.copy()
corrected_april_2["plot_index"] = plot_index

###Station 1###
#observed_april_2 = observed_april_2.head(210)
#simulated_april_2 = simulated_april_2.head(210)
#corrected_april_2 = corrected_april_2.head(210)

###Station 2###
#observed_april_2 = observed_april_2.head(180)
#simulated_april_2 = simulated_april_2.head(180)
#corrected_april_2 = corrected_april_2.head(180)
#observed_april_2 = observed_april_2.iloc[60:]
#simulated_april_2 = simulated_april_2.iloc[60:]
#corrected_april_2 = corrected_april_2.iloc[60:]

###Station 3###
#observed_april_2 = observed_april_2.head(180)
#simulated_april_2 = simulated_april_2.head(180)
#corrected_april_2 = corrected_april_2.head(180)

###Station 4###
#observed_april_2 = observed_april_2.head(450)
#simulated_april_2 = simulated_april_2.head(450)
#corrected_april_2 = corrected_april_2.head(450)
#observed_april_2 = observed_april_2.iloc[330:]
#simulated_april_2 = simulated_april_2.iloc[330:]
#corrected_april_2 = corrected_april_2.iloc[330:]

###Station 5###
#observed_april_2 = observed_april_2.head(180)
#simulated_april_2 = simulated_april_2.head(180)
#corrected_april_2 = corrected_april_2.head(180)

observed_april_3 = observed_april_2.dropna()

# Plotting the first graph (top-left)
axs[0, 0].plot(simulated_april_2["plot_index"], simulated_april_2[retro_input], label='Simulated Streamflow', color='#EF553B')
axs[0, 0].set_title('Simulated Hydrograph for April (2009-2025)', fontweight='bold') #Station 1
axs[0, 0].set_ylabel('Streamflow (m³/s)')
axs[0, 0].set_xlabel('Time')  # Adding x-label
axs[0, 0].legend()
axs[0, 0].grid(True)  # Add grid
axs[0, 0].set_ylim(0, 51000)  #Station 1  # Set y-limit for the upper left plot
#axs[0, 0].set_ylim(0, 1300)  #Station 2  # Set y-limit for the upper left plot
#axs[0, 0].set_ylim(0, 3500)  #Station 3  # Set y-limit for the upper left plot
#axs[0, 0].set_ylim(0, 1600)  #Station 4  # Set y-limit for the upper left plot
#axs[0, 0].set_ylim(0, 31000)  #Station 5  # Set y-limit for the upper left plot

# Plotting the second graph (top-right)
axs[0, 1].plot(simcdf, sim_bin_edges, label='Sim. FDC', color='#EF553B')
axs[0, 1].set_title('Flow Duration Curve For The Month of April', fontweight='bold')
axs[0, 1].set_ylabel('Streamflow (m³/s)')
axs[0, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[0, 1].legend()
axs[0, 1].grid(True)  # Add grid
axs[0, 1].set_ylim(0, 51000)  #Station 1  # Set y-limit for the upper left plot
#axs[0, 1].set_ylim(0, 1300)  #Station 2  # Set y-limit for the upper left plot
#axs[0, 1].set_ylim(0, 3500)  #Station 3  # Set y-limit for the upper left plot
#axs[0, 1].set_ylim(0, 1600)  #Station 4  # Set y-limit for the upper left plot
#axs[0, 1].set_ylim(0, 31000)  #Station 4  # Set y-limit for the upper left plot

# Plotting the third graph (bottom-left)
axs[1, 0].plot(observed_april_3["plot_index"], observed_april_3['Water Level (m)'], label='Observed Water Level', color='#ff7f0e', linestyle='--', marker='o')
axs[1, 0].plot(corrected_april_2["plot_index"], corrected_april_2['Transformed Water Level (m)'], label='Transformed Water Level', color='#1f77b4')
axs[1, 0].set_title('Observed and Transformed Water Level values for April (2009-2025)', fontweight='bold') #Station 1
axs[1, 0].set_ylabel('Water Level (m)')
axs[1, 0].set_xlabel('Time')  # Adding x-label
axs[1, 0].legend()
axs[1, 0].grid(True)  # Add grid
axs[1, 0].set_ylim(67, 76)  #Station 1  # Set y-limit for the upper left plot
#axs[1, 0].set_ylim(129, 136)  #Station 2  # Set y-limit for the upper left plot
#axs[1, 0].set_ylim(127, 135)  #Station 3  # Set y-limit for the upper left plot
#axs[1, 0].set_ylim(48, 53)  #Station 4  # Set y-limit for the upper left plot
#axs[1, 0].set_ylim(429, 438)  #Station 5  # Set y-limit for the upper left plot

# Plotting the fourth graph (bottom-right)
axs[1, 1].plot(obscdf, obs_bin_edges, label='Obs. WLDC', color='#ff7f0e', linestyle='--')
#axs[1, 1].plot(corcdf, cor_bin_edges, label='Sim. WLDC', color='#1f77b4')
axs[1, 1].set_title('Water Level Duration Curve For The Month of April', fontweight='bold')
axs[1, 1].set_ylabel('Water Level (m)')
axs[1, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[1, 1].legend()
axs[1, 1].grid(True)  # Add grid
axs[1, 1].set_ylim(67, 76)  #Station 1  # Set y-limit for the upper left plot
#axs[1, 1].set_ylim(129, 136)  #Station 2  # Set y-limit for the upper left plot
#axs[1, 1].set_ylim(127, 135)  #Station 3  # Set y-limit for the upper left plot
#axs[1, 1].set_ylim(48, 53)  #Station 3  # Set y-limit for the upper left plot
#axs[1, 1].set_ylim(429, 438)  #Station 3  # Set y-limit for the upper left plot

# Adjusting x-axis dates for upper-left and lower-left plots
for ax in axs.flat[[0, 2]]:
    ax.xaxis.set_major_locator(plt.MaxNLocator(7))  # Adjust the number of ticks as needed

# Automatically adjust space between plots
plt.tight_layout()

#interpolating functions
f_obs = interpolate.interp1d(obs_bin_edges,obscdf)
f_sim = interpolate.interp1d(sim_bin_edges,simcdf)
f_cor = interpolate.interp1d(cor_bin_edges,corcdf)

f_obs_inv = interpolate.interp1d(obscdf, obs_bin_edges)
f_sim_inv = interpolate.interp1d(simcdf, sim_bin_edges)
f_cor_inv = interpolate.interp1d(corcdf, cor_bin_edges)

# Plotting horizontal lines connecting upper-left and upper-right plots
#Station 1
point1_x = simulated_april_2["plot_index"][239]  #point in upper left plot
point1_y = simulated_april_2['{}'.format(retro_input)][239]  #point in upper left plot
point2_x = f_sim(point1_y)  #point in upper right plot
point2_y = simulated_april_2['{}'.format(retro_input)][239]  #point in upper right plot
point3_x = point2_x  #point in lower right plot
point3_y = ((f_obs_inv(point2_x))+(corrected_april_2['Transformed Water Level (m)'][239])) / 2
point4_x = simulated_april_2["plot_index"][239]  #point in lower left plot
point4_y = point3_y  #point in lower left plot

#Station 2
#point1_x = simulated_april_2["plot_index"][146]  #point in upper left plot
#point1_y = simulated_april_2['{}'.format(retro_input)][146]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = simulated_april_2['{}'.format(retro_input)][146]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = ((f_obs_inv(point2_x))+(corrected_april_2['Transformed Water Level (m)'][146])) / 2
#point4_x = simulated_april_2["plot_index"][146]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

#Station 3
#point1_x = simulated_april_2["plot_index"][65]  #point in upper left plot
#point1_y = simulated_april_2['{}'.format(retro_input)][65]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = simulated_april_2['{}'.format(retro_input)][65]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = ((f_obs_inv(point2_x))+(corrected_april_2['Transformed Water Level (m)'][65])) / 2
#point4_x = simulated_april_2["plot_index"][65]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

#Station 4
#point1_x = simulated_april_2["plot_index"][388]  #point in upper left plot
#point1_y = simulated_april_2['{}'.format(retro_input)][388]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = simulated_april_2['{}'.format(retro_input)][388]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = ((f_obs_inv(point2_x))+(corrected_april_2['Transformed Water Level (m)'][388])) / 2
#point4_x = simulated_april_2["plot_index"][388]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

#Station 5
#point1_x = simulated_april_2["plot_index"][60]  #point in upper left plot
#point1_y = simulated_april_2['{}'.format(retro_input)][60]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = simulated_april_2['{}'.format(retro_input)][60]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = ((f_obs_inv(point2_x))+(corrected_april_2['Transformed Water Level (m)'][60])) / 2
#point4_x = simulated_april_2["plot_index"][60]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

# Plotting horizontal line with markers
#axs[0, 0].text(point1_x, point1_y+50, '1', color='black', ha='center', va='bottom') #Station 1
#axs[0, 1].text(point2_x, point2_y+250, '2', color='black', ha='left', va='bottom') #Station 1
#axs[0, 0].text(point1_x, point1_y+25, '1', color='black', ha='center', va='bottom') #Station 2
#axs[0, 1].text(point2_x, point2_y+25, '2', color='black', ha='left', va='bottom') #Station 2
axs[0, 0].text(point1_x, point1_y+25, '1', color='black', ha='center', va='bottom') #Station 3-4
axs[0, 1].text(point2_x, point2_y+25, '2', color='black', ha='right', va='bottom') #Station 3-4
axs[1, 1].text(point3_x, point3_y+0.20, '3', color='black', ha='right', va='bottom')
axs[1, 0].text(point4_x, point4_y+0.15, '4', color='black', ha='center', va='bottom')

# Adding points at specified locations
axs[0, 0].scatter(point1_x, point1_y, color='black', s=10)
axs[0, 1].scatter(point2_x, point2_y, color='black', s=10)
axs[1, 1].scatter(point3_x, point3_y, color='black', s=10)
axs[1, 0].scatter(point4_x, point4_y, color='black', s=10)

#plt.savefig('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Plots/DWLT Method {}.png'.format(name), dpi=700)
plt.savefig('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Plots\\DWLT Method {}.png'.format(name), dpi=700)

# Show the plots
plt.show()