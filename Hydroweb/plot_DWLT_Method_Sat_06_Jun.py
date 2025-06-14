import math
import numpy as np
import pandas as pd
import datetime as dt
import hydrostats.data
from scipy import interpolate
import matplotlib.pyplot as plt

'''Input Data'''
###Station 1###
#obs_input = '604-0031-659-037'
#retro_input = '621109666'
#name = 'Amazonas_xingu_km1020'

###Station 2###
#obs_input = '412-0492-015-022'
#retro_input = '441292437'
#name = 'Indus_indus_km0949'

###Station 3###
obs_input = '605-0031-223-014'
retro_input = '620953148'
name = 'Amazonas_guapore_km3139'


# Get Observed Data
#observed_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Observed_Hydroweb/{0}.csv'.format(obs_input), index_col=0)
observed_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Observed_Hydroweb\\{0}.csv'.format(obs_input), index_col=0)
observed_values.index = pd.to_datetime(observed_values.index)
observed_june = observed_values[observed_values.index.month == 6]

dayavg_obs_june = hydrostats.data.daily_average(observed_june, rolling=True)

# Get Simulated Data
#simulated_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Simulated_Data/GEOGLOWS_v2/{0}.csv'.format(retro_input), index_col=0)
simulated_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Simulated_Data\\GEOGLOWS_v2\\{0}.csv'.format(retro_input), index_col=0)
simulated_values.index = pd.to_datetime(simulated_values.index)
simulated_june = simulated_values[simulated_values.index.month == 6]
simulated_june_2 = simulated_june.loc[simulated_june.index >= pd.to_datetime(dt.datetime(observed_june.index[0].year, observed_june.index[0].month, 1))]

dayavg_sim_june = hydrostats.data.daily_average(simulated_june_2, rolling=True)

# Get Corrected Data
#corrected_values = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/DWLT/GEOGLOWS_v2/{0}-{1}_WL.csv'.format(obs_input, retro_input), index_col=0)
corrected_values = pd.read_csv('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\DWLT\\GEOGLOWS_v2\\{0}-{1}_WL.csv'.format(obs_input, retro_input), index_col=0)
corrected_values.index = pd.to_datetime(corrected_values.index)
corrected_june = corrected_values[corrected_values.index.month == 6]
corrected_june_2 = corrected_june.loc[corrected_june.index >= pd.to_datetime(dt.datetime(observed_june.index[0].year, observed_june.index[0].month, 1))]

dayavg_corr_june = hydrostats.data.daily_average(corrected_june_2, rolling=True)

# Get Observed FDC
monObs = observed_june.dropna()
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
monSim = simulated_june.dropna()
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
monCor = corrected_june.dropna()
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
date_range = pd.date_range(start=simulated_june_2.index.min(), end=simulated_june_2.index.max(), freq='D')
observed_june_2 = observed_june.reindex(date_range)
observed_june_2.index.name = 'Datetime'
observed_june_2 = observed_june_2[observed_june_2.index.month == 6]

# Usamos directamente el índice actual del dataframe, sin importar qué fechas son
plot_index = range(1, len(observed_june_2) + 1)

# Asignar índice artificial a cada DataFrame (todos tienen igual longitud tras reindexado)

observed_june_2 = observed_june_2.copy()
observed_june_2["plot_index"] = plot_index

simulated_june_2 = simulated_june_2.copy()
simulated_june_2["plot_index"] = plot_index

corrected_june_2 = corrected_june_2.copy()
corrected_june_2["plot_index"] = plot_index

observed_june_3 = observed_june_2.dropna()

# Plotting the first graph (top-left)
axs[0, 0].plot(simulated_june_2["plot_index"], simulated_june_2[retro_input], label='Simulated Streamflow', color='#EF553B')
axs[0, 0].set_title('Simulated Hydrograph for June (2009-2024)', fontweight='bold') #Station 1
axs[0, 0].set_ylabel('Streamflow (m³/s)')
axs[0, 0].set_xlabel('Time')  # Adding x-label
axs[0, 0].legend()
axs[0, 0].grid(True)  # Add grid
#axs[0, 0].set_ylim(0, 12000)  #Station 1  # Set y-limit for the upper left plot
#axs[0, 0].set_ylim(0, 32000)  #Station 2  # Set y-limit for the upper left plot
axs[0, 0].set_ylim(0, 12500)  #Station 3  # Set y-limit for the upper left plot


# Plotting the second graph (top-right)
axs[0, 1].plot(simcdf, sim_bin_edges, label='Sim. FDC', color='#EF553B')
axs[0, 1].set_title('Flow Duration Curve For The Month of June', fontweight='bold')
axs[0, 1].set_ylabel('Streamflow (m³/s)')
axs[0, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[0, 1].legend()
axs[0, 1].grid(True)  # Add grid
#axs[0, 1].set_ylim(0, 12000)  #Station 1  # Set y-limit for the upper left plot
#axs[0, 1].set_ylim(0, 32000)  #Station 2  # Set y-limit for the upper left plot
axs[0, 1].set_ylim(0, 12500)  #Station 3  # Set y-limit for the upper left plot


# Plotting the third graph (bottom-left)
axs[1, 0].plot(observed_june_3["plot_index"], observed_june_3['Water Level (m)'], label='Observed Water Level', color='#ff7f0e', linestyle='--', marker='o')
axs[1, 0].plot(corrected_june_2["plot_index"], corrected_june_2['Transformed Water Level (m)'], label='Transformed Water Level', color='#1f77b4')
axs[1, 0].set_title('Observed and Transformed Water Level values for June (2009-2024)', fontweight='bold') #Station 1
axs[1, 0].set_ylabel('Water Level (m)')
axs[1, 0].set_xlabel('Time')  # Adding x-label
axs[1, 0].legend()
axs[1, 0].grid(True)  # Add grid
#axs[1, 0].set_ylim(168, 173)  #Station 1  # Set y-limit for the upper left plot
#axs[1, 0].set_ylim(80, 85)  #Station 2  # Set y-limit for the upper left plot
axs[1, 0].set_ylim(132, 138)  #Station 3  # Set y-limit for the upper left plot


# Plotting the fourth graph (bottom-right)
axs[1, 1].plot(obscdf, obs_bin_edges, label='Obs. WLDC', color='#ff7f0e', linestyle='--')
#axs[1, 1].plot(corcdf, cor_bin_edges, label='Sim. WLDC', color='#1f77b4')
axs[1, 1].set_title('Water Level Duration Curve For The Month of June', fontweight='bold')
axs[1, 1].set_ylabel('Water Level (m)')
axs[1, 1].set_xlabel('Nonexceedance Probability')  # Adding x-label
axs[1, 1].legend()
axs[1, 1].grid(True)  # Add grid
#axs[1, 1].set_ylim(168, 173)  #Station 1  # Set y-limit for the upper left plot
#axs[1, 1].set_ylim(80, 85)  #Station 2  # Set y-limit for the upper left plot
axs[1, 1].set_ylim(132, 138)  #Station 3  # Set y-limit for the upper left plot


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
#point1_x = simulated_june_2["plot_index"][240]  #point in upper left plot
#point1_y = simulated_june_2['{}'.format(retro_input)][240]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = simulated_june_2['{}'.format(retro_input)][240]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = ((f_obs_inv(point2_x))+(corrected_june_2['Transformed Water Level (m)'][240])) / 2
#point4_x = simulated_june_2["plot_index"][240]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

#Station 2
#point1_x = simulated_june_2["plot_index"][148]  #point in upper left plot
#point1_y = simulated_june_2['{}'.format(retro_input)][148]  #point in upper left plot
#point2_x = f_sim(point1_y)  #point in upper right plot
#point2_y = simulated_june_2['{}'.format(retro_input)][148]  #point in upper right plot
#point3_x = point2_x  #point in lower right plot
#point3_y = ((f_obs_inv(point2_x))+(corrected_june_2['Transformed Water Level (m)'][148])) / 2
#point4_x = simulated_june_2["plot_index"][148]  #point in lower left plot
#point4_y = point3_y  #point in lower left plot

#Station 3
point1_x = simulated_june_2["plot_index"][95]  #point in upper left plot
point1_y = simulated_june_2['{}'.format(retro_input)][95]  #point in upper left plot
point2_x = f_sim(point1_y)  #point in upper right plot
point2_y = simulated_june_2['{}'.format(retro_input)][95]  #point in upper right plot
point3_x = point2_x  #point in lower right plot
point3_y = ((f_obs_inv(point2_x))+(corrected_june_2['Transformed Water Level (m)'][95])) / 2
point4_x = simulated_june_2["plot_index"][95]  #point in lower left plot
point4_y = point3_y  #point in lower left plot

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

if name == 'Amazonas_xingu_km1020':
    #plt.savefig('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Plots/DWLT Method {}.png'.format(name), dpi=700)
    plt.savefig('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Plots\\DWLT Method {}_v2.png'.format(name), dpi=700)
elif name == 'Amazonas_guapore_km3139':
    #plt.savefig('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Plots/DWLT Method {}.png'.format(name), dpi=700)
    plt.savefig('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Plots\\DWLT Method {}_v2.png'.format(name), dpi=700)
else:
    # plt.savefig('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Plots/DWLT Method {}.png'.format(name), dpi=700)
    plt.savefig('G:\\My Drive\\Personal_Files\\Post_Doc\\Hydroweb\\Plots\\DWLT Method {}.png'.format(name), dpi=700)

# Show the plots
plt.show()