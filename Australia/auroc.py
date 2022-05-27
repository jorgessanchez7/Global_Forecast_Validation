import io
import math
import warnings
import requests
import geoglows
import numpy as np
import pandas as pd
import hydrostats.data as hd
from sklearn.metrics import roc_auc_score, roc_curve

warnings.filterwarnings('ignore')

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
	#xbar = statistics.mean(year_max_flow_list)
	#std = statistics.stdev(year_max_flow_list, xbar=xbar)
	return -math.log(-math.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std)

return_periods = [100, 50, 25, 10, 5, 2]

auroc_array = []

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/Selected_Stations_Australia_Q.csv')

CODEs = stations_pd['Code'].tolist()
IDs = stations_pd['ts_id'].tolist()
#IDs = stations_pd['ts_id_water_level'].tolist() #water level
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Station'].tolist()


for id, code, name, comid in zip(IDs, CODEs, Names, COMIDs):

	comid = int(comid)

	print(id, ' - ', code, ' - ', name, ' - ', comid)

	historical_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/Observed_Data/{}.csv'.format(code), index_col=0)
	historical_df[historical_df < 0] = 0
	historical_df.index = pd.to_datetime(historical_df.index)
	historical_df.index = historical_df.index.to_series().dt.strftime("%Y-%m-%d")
	historical_df.index = pd.to_datetime(historical_df.index)
	historical_df = historical_df.dropna()

	'''Reading the simulated data'''

	# Original Simulated Data
	simulated_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/Simulated_Data/{}.csv'.format(comid), index_col=0)

	# Bias Corrected Simulated Data
	#simulated_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/Corrected_Data/{0}-{1}.csv'.format(code, comid), index_col=0)

	simulated_df[simulated_df < 0] = 0
	simulated_df.index = pd.to_datetime(simulated_df.index)
	simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
	simulated_df.index = pd.to_datetime(simulated_df.index)

	'''Adding Return Periods for Observed Data'''
	max_annual_flow = historical_df.groupby(historical_df.index.strftime("%Y")).max()
	mean_value = np.mean(max_annual_flow.iloc[:,0].values)
	std_value = np.std(max_annual_flow.iloc[:,0].values)

	return_periods_values = []

	for rp in return_periods:
		return_periods_values.append(gumbel_1(std_value, mean_value, rp))

	d = {'station': [name], 'return_period_100': [return_periods_values[0]], 'return_period_50': [return_periods_values[1]], 'return_period_25': [return_periods_values[2]], 'return_period_10': [return_periods_values[3]], 'return_period_5': [return_periods_values[4]], 'return_period_2': [return_periods_values[5]]}
	rperiods = pd.DataFrame(data=d)
	rperiods.set_index('station', inplace=True)

	globals()['{}_RP'.format(name)] = rperiods.copy()

	observed_rp = pd.concat([globals()['{}_RP'.format(name)]])

	'''Adding Return Periods for Simulated Data'''
	max_annual_flow = simulated_df.groupby(simulated_df.index.strftime("%Y")).max()
	mean_value = np.mean(max_annual_flow.iloc[:, 0].values)
	std_value = np.std(max_annual_flow.iloc[:, 0].values)

	return_periods_values = []

	for rp in return_periods:
		return_periods_values.append(gumbel_1(std_value, mean_value, rp))

	d = {'station': [comid], 'return_period_100': [return_periods_values[0]],
	     'return_period_50': [return_periods_values[1]], 'return_period_25': [return_periods_values[2]],
	     'return_period_10': [return_periods_values[3]], 'return_period_5': [return_periods_values[4]],
	     'return_period_2': [return_periods_values[5]]}
	rperiods = pd.DataFrame(data=d)
	rperiods.set_index('station', inplace=True)

	globals()['{}_RP'.format(comid)] = rperiods.copy()

	simulated_rp = pd.concat([globals()['{}_RP'.format(comid)]])

	'''Defining Return Period for Observed Streamflow'''

	obs_2_threshold = observed_rp['return_period_2'].loc[observed_rp.index == '{0}'.format(name)].values[0]
	obs_5_threshold = observed_rp['return_period_5'].loc[observed_rp.index == '{0}'.format(name)].values[0]
	obs_10_threshold = observed_rp['return_period_10'].loc[observed_rp.index == '{0}'.format(name)].values[0]
	obs_25_threshold = observed_rp['return_period_25'].loc[observed_rp.index == '{0}'.format(name)].values[0]
	obs_50_threshold = observed_rp['return_period_50'].loc[observed_rp.index == '{0}'.format(name)].values[0]
	obs_100_threshold = observed_rp['return_period_100'].loc[observed_rp.index == '{0}'.format(name)].values[0]

	'''Defining the simulated return periods thresholds'''

	sim_2_threshold = simulated_rp['return_period_2'].loc[simulated_rp.index == float('{0}.0'.format(comid))].values[0]
	sim_5_threshold = simulated_rp['return_period_5'].loc[simulated_rp.index == float('{0}.0'.format(comid))].values[0]
	sim_10_threshold = simulated_rp['return_period_10'].loc[simulated_rp.index == float('{0}.0'.format(comid))].values[0]
	sim_25_threshold = simulated_rp['return_period_25'].loc[simulated_rp.index == float('{0}.0'.format(comid))].values[0]
	sim_50_threshold = simulated_rp['return_period_50'].loc[simulated_rp.index == float('{0}.0'.format(comid))].values[0]
	sim_100_threshold = simulated_rp['return_period_100'].loc[simulated_rp.index == float('{0}.0'.format(comid))].values[0]

	merged_df = hd.merge_data(sim_df=simulated_df, obs_df=historical_df)

	historical_df = merged_df.iloc[:, 1].to_frame()
	simulated_df = merged_df.iloc[:, 0].to_frame()

	df1 = historical_df.loc[(historical_df['Observed'] < obs_2_threshold)]
	df2 = historical_df.loc[(historical_df['Observed'] >= obs_2_threshold)]
	df3 = historical_df.loc[(historical_df['Observed'] >= obs_5_threshold)]
	df4 = historical_df.loc[(historical_df['Observed'] >= obs_10_threshold)]
	df5 = historical_df.loc[(historical_df['Observed'] >= obs_25_threshold)]
	df6 = historical_df.loc[(historical_df['Observed'] >= obs_50_threshold)]
	df7 = historical_df.loc[(historical_df['Observed'] >= obs_100_threshold)]

	event_return_period = 0

	if len(df2.index) > 0:
		event_return_period = 2
		if len(df3.index) > 0:
			event_return_period = 5
			if len(df4.index) > 0:
				event_return_period = 10
				if len(df5.index) > 0:
					event_return_period = 25
					if len(df6.index) > 0:
						event_return_period = 50
						if len(df7.index) > 0:
							event_return_period = 100

	#print('La magnitud del caudal maximo en la serie de tiempo de la estacion {0} sobrepasa el período de retorno de {1} años'.format(id, event_return_period))

	'''Calculating the AUROC'''
	
	if event_return_period == 0:
		print('No hay evento de inundación a ser analizado')
		AUROC_ini = np.nan

	elif event_return_period == 2:
		# Converting observed data to binary
		obs_bin = historical_df.copy()
		obs_bin.loc[obs_bin['Observed'] < obs_2_threshold] = 0
		obs_bin.loc[obs_bin['Observed'] >= obs_2_threshold] = 1

		# Converting simulated data data to binary
		forecast_ini_bin = simulated_df.copy()
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] < sim_2_threshold] = 0
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] >= sim_2_threshold] = 1

		merged_df_ini = pd.DataFrame.join(forecast_ini_bin, obs_bin).dropna()

		AUROC = round(roc_auc_score(merged_df_ini['Observed'].tolist(), merged_df_ini['Simulated'].tolist()), 4)
		AUROC_ini = AUROC

		#print('El área bajo la curva ROC en la inicialización del pronóstico en la estación {0} es: {1}'.format(name, AUROC))

	elif event_return_period == 5:
		# Converting observed data to binary
		obs_bin = historical_df.copy()
		obs_bin.loc[obs_bin['Observed'] < obs_2_threshold] = 0
		obs_bin.loc[(obs_bin['Observed'] >= obs_2_threshold) & (obs_bin['Observed'] < obs_5_threshold)] = 1
		obs_bin.loc[obs_bin['Observed'] >= obs_5_threshold] = 2

		# Converting simulated data data to binary
		forecast_ini_bin = simulated_df.copy()
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] < sim_2_threshold] = 0
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_2_threshold) & (forecast_ini_bin['Simulated'] < sim_5_threshold)] = 1
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] >= sim_5_threshold] = 2

		merged_df_ini = pd.DataFrame.join(forecast_ini_bin, obs_bin).dropna()

		fpr, tpr, _ = roc_curve(merged_df_ini['Observed'].tolist(), merged_df_ini['Simulated'].tolist(), pos_label=2)

		AUROC = 0

		for i in range(1, len(fpr)):
			if i == 1:
				AUROC = AUROC + ((fpr[i] * tpr[i]) / 2)
			else:
				AUROC = AUROC + ((tpr[i] + tpr[i - 1]) / 2) * (fpr[i] - fpr[i - 1])

		AUROC = round(AUROC, 4)
		AUROC_ini = AUROC

		#print('El área bajo la curva ROC en la inicialización del pronóstico en la estación {0} es: {1}'.format(name, AUROC))

	elif event_return_period == 10:
		# Converting observed data to binary
		obs_bin = historical_df.copy()
		obs_bin.loc[obs_bin['Observed'] < obs_2_threshold] = 0
		obs_bin.loc[(obs_bin['Observed'] >= obs_2_threshold) & (obs_bin['Observed'] < obs_5_threshold)] = 1
		obs_bin.loc[(obs_bin['Observed'] >= obs_5_threshold) & (obs_bin['Observed'] < obs_10_threshold)] = 2
		obs_bin.loc[obs_bin['Observed'] >= obs_10_threshold] = 3

		# Converting simulated data data to binary
		forecast_ini_bin = simulated_df.copy()
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] < sim_2_threshold] = 0
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_2_threshold) & (forecast_ini_bin['Simulated'] < sim_5_threshold)] = 1
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_5_threshold) & (forecast_ini_bin['Simulated'] < sim_10_threshold)] = 2
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] >= sim_10_threshold] = 3

		merged_df_ini = pd.DataFrame.join(forecast_ini_bin, obs_bin).dropna()

		fpr, tpr, _ = roc_curve(merged_df_ini['Observed'].tolist(), merged_df_ini['Simulated'].tolist(), pos_label=3)

		AUROC = 0

		for i in range(1, len(fpr)):
			if i == 1:
				AUROC = AUROC + ((fpr[i] * tpr[i]) / 2)
			else:
				AUROC = AUROC + ((tpr[i] + tpr[i - 1]) / 2) * (fpr[i] - fpr[i - 1])

		AUROC = round(AUROC, 4)
		AUROC_ini = AUROC

		#print('El área bajo la curva ROC en la inicialización del pronóstico en la estación {0} es: {1}'.format(name, AUROC))

	elif event_return_period == 25:
		# Converting observed data to binary
		obs_bin = historical_df.copy()
		obs_bin.loc[obs_bin['Observed'] < obs_2_threshold] = 0
		obs_bin.loc[(obs_bin['Observed'] >= obs_2_threshold) & (obs_bin['Observed'] < obs_5_threshold)] = 1
		obs_bin.loc[(obs_bin['Observed'] >= obs_5_threshold) & (obs_bin['Observed'] < obs_10_threshold)] = 2
		obs_bin.loc[(obs_bin['Observed'] >= obs_10_threshold) & (obs_bin['Observed'] < obs_25_threshold)] = 3
		obs_bin.loc[obs_bin['Observed'] >= obs_25_threshold] = 4

		# Converting simulated data data to binary
		forecast_ini_bin = simulated_df.copy()
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] < sim_2_threshold] = 0
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_2_threshold) & (forecast_ini_bin['Simulated'] < sim_5_threshold)] = 1
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_5_threshold) & (forecast_ini_bin['Simulated'] < sim_10_threshold)] = 2
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_10_threshold) & (forecast_ini_bin['Simulated'] < sim_25_threshold)] = 3
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] >= sim_25_threshold] = 4

		merged_df_ini = pd.DataFrame.join(forecast_ini_bin, obs_bin).dropna()

		fpr, tpr, _ = roc_curve(merged_df_ini['Observed'].tolist(), merged_df_ini['Simulated'].tolist(), pos_label=4)

		AUROC = 0

		for i in range(1, len(fpr)):
			if i == 1:
				AUROC = AUROC + ((fpr[i] * tpr[i]) / 2)
			else:
				AUROC = AUROC + ((tpr[i] + tpr[i - 1]) / 2) * (fpr[i] - fpr[i - 1])

		AUROC = round(AUROC, 4)
		AUROC_ini = AUROC

		#print('El área bajo la curva ROC  en la inicialización del pronóstico en la estación {0} es: {1}'.format(name, AUROC))

	elif event_return_period == 50:
		# Converting observed data to binary
		obs_bin = historical_df.copy()
		obs_bin.loc[obs_bin['Observed'] < obs_2_threshold] = 0
		obs_bin.loc[(obs_bin['Observed'] >= obs_2_threshold) & (obs_bin['Observed'] < obs_5_threshold)] = 1
		obs_bin.loc[(obs_bin['Observed'] >= obs_5_threshold) & (obs_bin['Observed'] < obs_10_threshold)] = 2
		obs_bin.loc[(obs_bin['Observed'] >= obs_10_threshold) & (obs_bin['Observed'] < obs_25_threshold)] = 3
		obs_bin.loc[(obs_bin['Observed'] >= obs_25_threshold) & (obs_bin['Observed'] < obs_50_threshold)] = 4
		obs_bin.loc[obs_bin['Observed'] >= obs_50_threshold] = 5

		# Converting simulated data data to binary
		forecast_ini_bin = simulated_df.copy()
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] < sim_2_threshold] = 0
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_2_threshold) & (forecast_ini_bin['Simulated'] < sim_5_threshold)] = 1
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_5_threshold) & (forecast_ini_bin['Simulated'] < sim_10_threshold)] = 2
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_10_threshold) & (forecast_ini_bin['Simulated'] < sim_25_threshold)] = 3
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_25_threshold) & (forecast_ini_bin['Simulated'] < sim_50_threshold)] = 4
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] >= sim_50_threshold] = 5

		merged_df_ini = pd.DataFrame.join(forecast_ini_bin, obs_bin).dropna()

		fpr, tpr, _ = roc_curve(merged_df_ini['Observed'].tolist(), merged_df_ini['Simulated'].tolist(), pos_label=5)

		AUROC = 0

		for i in range(1, len(fpr)):
			if i == 1:
				AUROC = AUROC + ((fpr[i] * tpr[i]) / 2)
			else:
				AUROC = AUROC + ((tpr[i] + tpr[i - 1]) / 2) * (fpr[i] - fpr[i - 1])

		AUROC = round(AUROC, 4)
		AUROC_ini = AUROC

		#print('El área bajo la curva ROC en la inicialización del pronóstico en la estación {0} es: {1}'.format(name, AUROC))

	elif event_return_period == 100:
		# Converting observed data to binary
		obs_bin = historical_df.copy()
		obs_bin.loc[obs_bin['Observed'] < obs_2_threshold] = 0
		obs_bin.loc[(obs_bin['Observed'] >= obs_2_threshold) & (obs_bin['Observed'] < obs_5_threshold)] = 1
		obs_bin.loc[(obs_bin['Observed'] >= obs_5_threshold) & (obs_bin['Observed'] < obs_10_threshold)] = 2
		obs_bin.loc[(obs_bin['Observed'] >= obs_10_threshold) & (obs_bin['Observed'] < obs_25_threshold)] = 3
		obs_bin.loc[(obs_bin['Observed'] >= obs_25_threshold) & (obs_bin['Observed'] < obs_50_threshold)] = 4
		obs_bin.loc[(obs_bin['Observed'] >= obs_50_threshold) & (obs_bin['Observed'] < obs_100_threshold)] = 5
		obs_bin.loc[obs_bin['Observed'] >= obs_100_threshold] = 6

		# Converting simulated data data to binary
		forecast_ini_bin = simulated_df.copy()
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] < sim_2_threshold] = 0
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_2_threshold) & (forecast_ini_bin['Simulated'] < sim_5_threshold)] = 1
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_5_threshold) & (forecast_ini_bin['Simulated'] < sim_10_threshold)] = 2
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_10_threshold) & (forecast_ini_bin['Simulated'] < sim_25_threshold)] = 3
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_25_threshold) & (forecast_ini_bin['Simulated'] < sim_50_threshold)] = 4
		forecast_ini_bin.loc[(forecast_ini_bin['Simulated'] >= sim_50_threshold) & (forecast_ini_bin['Simulated'] < sim_100_threshold)] = 5
		forecast_ini_bin.loc[forecast_ini_bin['Simulated'] >= sim_100_threshold] = 6

		merged_df_ini = pd.DataFrame.join(forecast_ini_bin, obs_bin).dropna()

		fpr, tpr, _ = roc_curve(merged_df_ini['Observed'].tolist(), merged_df_ini['Simulated'].tolist(), pos_label=6)

		AUROC = 0

		for i in range(1, len(fpr)):
			if i == 1:
				AUROC = AUROC + ((fpr[i] * tpr[i]) / 2)
			else:
				AUROC = AUROC + ((tpr[i] + tpr[i - 1]) / 2) * (fpr[i] - fpr[i - 1])

		AUROC = round(AUROC, 4)
		AUROC_ini = AUROC

		#print('El área bajo la curva ROC en la inicialización del pronóstico en la estación {0} es: {1}'.format(name, AUROC))

	auroc_array.append([id, comid, name, event_return_period, AUROC_ini])

auroc_df = pd.DataFrame(auroc_array, columns=['Code', 'COMID', 'Station', 'Return Period', 'AUROC'])
print(auroc_df)

#Original Simulated Data
auroc_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/validationResults_Original/Tables/AUROC_Before_Bias_Correction.csv')

#Bias Corrected Simulated Data
#auroc_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/Australia/Australia/data/historical/validationResults_Corrected/Tables/AUROC_After_Bias_Correction.csv')
