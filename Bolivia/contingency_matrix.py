import io
import math
import warnings
import geoglows
import requests
import numpy as np
import pandas as pd
import hydrostats.data as hd

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

contingency_matrix = []

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Bolivia/Selected_Stations_Bolivia_Q_v0.csv')

IDs = stations_pd['Codigo'].tolist()
COMIDs = stations_pd['COMID'].tolist()
Names = stations_pd['Estacion'].tolist()


for id, name, comid in zip(IDs, Names, COMIDs):

	comid = int(comid)

	print(id, ' - ', ' - ', name, ' - ', comid)

	historical_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Bolivia/data/historical/Observed_Data/{}.csv'.format(id), index_col=0)
	historical_df[historical_df < 0] = 0
	historical_df.index = pd.to_datetime(historical_df.index)
	historical_df.index = historical_df.index.to_series().dt.strftime("%Y-%m-%d")
	historical_df.index = pd.to_datetime(historical_df.index)
	historical_df = historical_df.dropna()

	'''Reading the simulated data'''

	# Original Simulated Data
	#simulated_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Bolivia/data/historical/Simulated_Data/{}.csv'.format(comid), index_col=0)

	# Bias Corrected Simulated Data
	simulated_df = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Bolivia/data/historical/Corrected_Data/{0}-{1}.csv'.format(id, comid), index_col=0)

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

	'''Creating Contingency Matrix'''
	
	if event_return_period == 0:
		print('No hay evento de inundación a ser analizado')
		success_rate_ini = np.nan
		overestimating_rate_ini = np.nan
		underestimating_rate_ini = np.nan
	
	elif event_return_period == 2:
		data = {'<2 years (obs)': [0.0000, 0.0000, 0.000], '>=2 years (obs)': [0.0000, 0.0000, 0.000],'Total (obs)': [0.0000, 0.0000, 0.000]}
		df = pd.DataFrame(data, index=['<2 years (sim)', '>=2 years (sim)', 'Total (sim)'])

		# Percentages
		success_rate = 0
		overestimating_rate = 0
		underestimating_rate = 0

		df1 = historical_df.loc[(historical_df['Observed'] < obs_2_threshold)]
		merged_df1 = pd.DataFrame.join(simulated_df, df1).dropna()
		merged_df1.drop('Observed', axis=1, inplace=True)

		if len(merged_df1.index) == 0:
			print('')
		else:
			sf1_1 = merged_df1.loc[(merged_df1['Simulated'] < sim_2_threshold)]
			sf1_2 = merged_df1.loc[(merged_df1['Simulated'] >= sim_2_threshold)]

			nf1_1 = round((len(sf1_1.index)) / (len(historical_df.index)), 4)
			nf1_2 = round((len(sf1_2.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf1_1
			overestimating_rate = overestimating_rate + nf1_2

			df.loc['<2 years (sim)', '<2 years (obs)'] = nf1_1
			df.loc['>=2 years (sim)', '<2 years (obs)'] = nf1_2
			df.loc['Total (sim)', '<2 years (obs)'] = nf1_1 + nf1_2

		df2 = historical_df.loc[(historical_df['Observed'] >= obs_2_threshold)]
		merged_df2 = pd.DataFrame.join(simulated_df, df2).dropna()
		merged_df2.drop('Observed', axis=1, inplace=True)

		if len(merged_df2.index) == 0:
			print('')
		else:
			sf2_1 = merged_df2.loc[(merged_df2['Simulated'] < sim_2_threshold)]
			sf2_2 = merged_df2.loc[(merged_df2['Simulated'] >= sim_2_threshold)]

			nf2_1 = round((len(sf2_1.index)) / (len(historical_df.index)), 4)
			nf2_2 = round((len(sf2_2.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf2_2
			underestimating_rate = underestimating_rate + nf2_1

			df.loc['<2 years (sim)', '>=2 years (obs)'] = nf2_1
			df.loc['>=2 years (sim)', '>=2 years (obs)'] = nf2_2
			df.loc['Total (sim)', '>=2 years (obs)'] = nf2_1 + nf2_2

		# Finishing filling matrix
		df.loc['<2 years (sim)', 'Total (obs)'] = df.loc['<2 years (sim)', '<2 years (obs)'] + df.loc['<2 years (sim)', '>=2 years (obs)']
		df.loc['>=2 years (sim)', 'Total (obs)'] = df.loc['>=2 years (sim)', '<2 years (obs)'] + df.loc['>=2 years (sim)', '>=2 years (obs)']
		df.loc['Total (sim)', 'Total (obs)'] = df.loc['Total (sim)', '<2 years (obs)'] + df.loc['Total (sim)', '>=2 years (obs)']

		success_rate_ini = success_rate
		overestimating_rate_ini = overestimating_rate
		underestimating_rate_ini = underestimating_rate

		#print(df)
		#print(' ')
		#print('El porcentaje deéxito para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((success_rate * 100), 2)) + '%')
		#print('El porcentaje desobre-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((overestimating_rate * 100), 2)) + '%')
		#print('El porcentaje desub-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((underestimating_rate * 100), 2)) + '%')
		#print(' ')
		#print(' ')

	elif event_return_period == 5:
		data = {'<2 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000], '2 years - 5 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000], '>=5 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000], 'Total (obs)': [0.0000, 0.0000, 0.0000, 0.0000]}
		df = pd.DataFrame(data, index=['<2 years (sim)', '2 years - 5 years (sim)', '>=5 years (sim)', 'Total (sim)'])

		# Percentages
		success_rate = 0
		overestimating_rate = 0
		underestimating_rate = 0

		df1 = historical_df.loc[(historical_df['Observed'] < obs_2_threshold)]
		merged_df1 = pd.DataFrame.join(simulated_df, df1).dropna()
		merged_df1.drop('Observed', axis=1, inplace=True)

		if len(merged_df1.index) == 0:
			print('')
		else:
			sf1_1 = merged_df1.loc[(merged_df1['Simulated'] < sim_2_threshold)]
			sf1_2 = merged_df1.loc[(merged_df1['Simulated'] >= sim_2_threshold) & (merged_df1['Simulated'] < sim_5_threshold)]
			sf1_3 = merged_df1.loc[(merged_df1['Simulated'] >= sim_5_threshold)]

			nf1_1 = round((len(sf1_1.index)) / (len(historical_df.index)), 4)
			nf1_2 = round((len(sf1_2.index)) / (len(historical_df.index)), 4)
			nf1_3 = round((len(sf1_3.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf1_1
			overestimating_rate = overestimating_rate + nf1_2 + nf1_3

			df.loc['<2 years (sim)', '<2 years (obs)'] = nf1_1
			df.loc['2 years - 5 years (sim)', '<2 years (obs)'] = nf1_2
			df.loc['>=5 years (sim)', '<2 years (obs)'] = nf1_3
			df.loc['Total (sim)', '<2 years (obs)'] = nf1_1 + nf1_2 + nf1_3

		df2 = historical_df.loc[(historical_df['Observed'] >= obs_2_threshold) & (historical_df['Observed'] < obs_5_threshold)]
		merged_df2 = pd.DataFrame.join(simulated_df, df2).dropna()
		merged_df2.drop('Observed', axis=1, inplace=True)

		if len(merged_df2.index) == 0:
			print('')
		else:
			sf2_1 = merged_df2.loc[(merged_df2['Simulated'] < sim_2_threshold)]
			sf2_2 = merged_df2.loc[(merged_df2['Simulated'] >= sim_2_threshold) & (merged_df2['Simulated'] < sim_5_threshold)]
			sf2_3 = merged_df2.loc[(merged_df2['Simulated'] >= sim_5_threshold)]

			nf2_1 = round((len(sf2_1.index)) / (len(historical_df.index)), 4)
			nf2_2 = round((len(sf2_2.index)) / (len(historical_df.index)), 4)
			nf2_3 = round((len(sf2_3.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf2_2
			overestimating_rate = overestimating_rate + nf2_3
			underestimating_rate = underestimating_rate + nf2_1

			df.loc['<2 years (sim)', '2 years - 5 years (obs)'] = nf2_1
			df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] = nf2_2
			df.loc['>=5 years (sim)', '2 years - 5 years (obs)'] = nf2_3
			df.loc['Total (sim)', '2 years - 5 years (obs)'] = nf2_1 + nf2_2 + nf2_3

		df3 = historical_df.loc[(historical_df['Observed'] >= obs_5_threshold)]
		merged_df3 = pd.DataFrame.join(simulated_df, df3).dropna()
		merged_df3.drop('Observed', axis=1, inplace=True)

		if len(merged_df3.index) == 0:
			print('')
		else:
			sf3_1 = merged_df3.loc[(merged_df3['Simulated'] < sim_2_threshold)]
			sf3_2 = merged_df3.loc[(merged_df3['Simulated'] >= sim_2_threshold) & (merged_df3['Simulated'] < sim_5_threshold)]
			sf3_3 = merged_df3.loc[(merged_df3['Simulated'] >= sim_5_threshold)]

			nf3_1 = round((len(sf3_1.index)) / (len(historical_df.index)), 4)
			nf3_2 = round((len(sf3_2.index)) / (len(historical_df.index)), 4)
			nf3_3 = round((len(sf3_3.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf3_3
			underestimating_rate = underestimating_rate + nf3_1 + nf3_2

			df.loc['<2 years (sim)', '>=5 years (obs)'] = nf3_1
			df.loc['2 years - 5 years (sim)', '>=5 years (obs)'] = nf3_2
			df.loc['>=5 years (sim)', '>=5 years (obs)'] = nf3_3
			df.loc['Total (sim)', '>=5 years (obs)'] = nf3_1 + nf3_2 + nf3_3

		# Finishing filling matrix
		df.loc['<2 years (sim)', 'Total (obs)'] = df.loc['<2 years (sim)', '<2 years (obs)'] + df.loc['<2 years (sim)', '2 years - 5 years (obs)'] + df.loc['<2 years (sim)', '>=5 years (obs)']
		df.loc['2 years - 5 years (sim)', 'Total (obs)'] = df.loc['2 years - 5 years (sim)', '<2 years (obs)'] + df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] + df.loc['2 years - 5 years (sim)', '>=5 years (obs)']
		df.loc['>=5 years (sim)', 'Total (obs)'] = df.loc['>=5 years (sim)', '<2 years (obs)'] + df.loc['>=5 years (sim)', '2 years - 5 years (obs)'] + df.loc['>=5 years (sim)', '>=5 years (obs)']
		df.loc['Total (sim)', 'Total (obs)'] = df.loc['Total (sim)', '<2 years (obs)'] + df.loc['Total (sim)', '2 years - 5 years (obs)'] + df.loc['Total (sim)', '>=5 years (obs)']

		success_rate_ini = success_rate
		overestimating_rate_ini = overestimating_rate
		underestimating_rate_ini = underestimating_rate

		#print(df)
		#print(' ')
		#print('El porcentaje deéxito para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((success_rate * 100), 2)) + '%')
		#print('El porcentaje desobre-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((overestimating_rate * 100), 2)) + '%')
		#print('El porcentaje desub-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((underestimating_rate * 100), 2)) + '%')
		#print(' ')
		#print(' ')

	elif event_return_period == 10:

		data = {'<2 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '2 years - 5 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '5 years - 10 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '>=10 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000], 'Total (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000]}
		df = pd.DataFrame(data, index=['<2 years (sim)', '2 years - 5 years (sim)', '5 years - 10 years (sim)', '>=10 years (sim)', 'Total (sim)'])

		# Percentages
		success_rate = 0
		overestimating_rate = 0
		underestimating_rate = 0

		df1 = historical_df.loc[(historical_df['Observed'] < obs_2_threshold)]
		merged_df1 = pd.DataFrame.join(simulated_df, df1).dropna()
		merged_df1.drop('Observed', axis=1, inplace=True)

		if len(merged_df1.index) == 0:
			print('')
		else:
			sf1_1 = merged_df1.loc[(merged_df1['Simulated'] < sim_2_threshold)]
			sf1_2 = merged_df1.loc[(merged_df1['Simulated'] >= sim_2_threshold) & (merged_df1['Simulated'] < sim_5_threshold)]
			sf1_3 = merged_df1.loc[(merged_df1['Simulated'] >= sim_5_threshold) & (merged_df1['Simulated'] < sim_10_threshold)]
			sf1_4 = merged_df1.loc[(merged_df1['Simulated'] >= sim_10_threshold)]

			nf1_1 = round((len(sf1_1.index)) / (len(historical_df.index)), 4)
			nf1_2 = round((len(sf1_2.index)) / (len(historical_df.index)), 4)
			nf1_3 = round((len(sf1_3.index)) / (len(historical_df.index)), 4)
			nf1_4 = round((len(sf1_4.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf1_1
			overestimating_rate = overestimating_rate + nf1_2 + nf1_3 + nf1_4

			df.loc['<2 years (sim)', '<2 years (obs)'] = nf1_1
			df.loc['2 years - 5 years (sim)', '<2 years (obs)'] = nf1_2
			df.loc['5 years - 10 years (sim)', '<2 years (obs)'] = nf1_3
			df.loc['>=10 years (sim)', '<2 years (obs)'] = nf1_4
			df.loc['Total (sim)', '<2 years (obs)'] = nf1_1 + nf1_2 + nf1_3 + nf1_4

		df2 = historical_df.loc[(historical_df['Observed'] >= obs_2_threshold) & (historical_df['Observed'] < obs_5_threshold)]
		merged_df2 = pd.DataFrame.join(simulated_df, df2).dropna()
		merged_df2.drop('Observed', axis=1, inplace=True)

		if len(merged_df2.index) == 0:
			print('')
		else:
			sf2_1 = merged_df2.loc[(merged_df2['Simulated'] < sim_2_threshold)]
			sf2_2 = merged_df2.loc[(merged_df2['Simulated'] >= sim_2_threshold) & (merged_df2['Simulated'] < sim_5_threshold)]
			sf2_3 = merged_df2.loc[(merged_df2['Simulated'] >= sim_5_threshold) & (merged_df2['Simulated'] < sim_10_threshold)]
			sf2_4 = merged_df2.loc[(merged_df2['Simulated'] >= sim_10_threshold)]

			nf2_1 = round((len(sf2_1.index)) / (len(historical_df.index)), 4)
			nf2_2 = round((len(sf2_2.index)) / (len(historical_df.index)), 4)
			nf2_3 = round((len(sf2_3.index)) / (len(historical_df.index)), 4)
			nf2_4 = round((len(sf2_4.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf2_2
			overestimating_rate = overestimating_rate + nf2_3 + nf2_4
			underestimating_rate = underestimating_rate + nf2_1

			df.loc['<2 years (sim)', '2 years - 5 years (obs)'] = nf2_1
			df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] = nf2_2
			df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] = nf2_3
			df.loc['>=10 years (sim)', '2 years - 5 years (obs)'] = nf2_4
			df.loc['Total (sim)', '2 years - 5 years (obs)'] = nf2_1 + nf2_2 + nf2_3 + nf2_4

		df3 = historical_df.loc[(historical_df['Observed'] >= obs_5_threshold) & (historical_df['Observed'] < obs_10_threshold)]
		merged_df3 = pd.DataFrame.join(simulated_df, df3).dropna()
		merged_df3.drop('Observed', axis=1, inplace=True)

		if len(merged_df3.index) == 0:
			print('')
		else:
			sf3_1 = merged_df3.loc[(merged_df3['Simulated'] < sim_2_threshold)]
			sf3_2 = merged_df3.loc[(merged_df3['Simulated'] >= sim_2_threshold) & (merged_df3['Simulated'] < sim_5_threshold)]
			sf3_3 = merged_df3.loc[(merged_df3['Simulated'] >= sim_5_threshold) & (merged_df3['Simulated'] < sim_10_threshold)]
			sf3_4 = merged_df3.loc[(merged_df3['Simulated'] >= sim_10_threshold)]

			nf3_1 = round((len(sf3_1.index)) / (len(historical_df.index)), 4)
			nf3_2 = round((len(sf3_2.index)) / (len(historical_df.index)), 4)
			nf3_3 = round((len(sf3_3.index)) / (len(historical_df.index)), 4)
			nf3_4 = round((len(sf3_4.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf3_3
			overestimating_rate = overestimating_rate + nf3_4
			underestimating_rate = underestimating_rate + nf3_1 + nf3_2

			df.loc['<2 years (sim)', '5 years - 10 years (obs)'] = nf3_1
			df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] = nf3_2
			df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] = nf3_3
			df.loc['>=10 years (sim)', '5 years - 10 years (obs)'] = nf3_4
			df.loc['Total (sim)', '5 years - 10 years (obs)'] = nf3_1 + nf3_2 + nf3_3 + nf3_4

		df4 = historical_df.loc[(historical_df['Observed'] >= obs_10_threshold)]
		merged_df4 = pd.DataFrame.join(simulated_df, df4).dropna()
		merged_df4.drop('Observed', axis=1, inplace=True)

		if len(merged_df4.index) == 0:
			print('')
		else:
			sf4_1 = merged_df4.loc[(merged_df4['Simulated'] < sim_2_threshold)]
			sf4_2 = merged_df4.loc[(merged_df4['Simulated'] >= sim_2_threshold) & (merged_df4['Simulated'] < sim_5_threshold)]
			sf4_3 = merged_df4.loc[(merged_df4['Simulated'] >= sim_5_threshold) & (merged_df4['Simulated'] < sim_10_threshold)]
			sf4_4 = merged_df4.loc[(merged_df4['Simulated'] >= sim_10_threshold)]

			nf4_1 = round((len(sf4_1.index)) / (len(historical_df.index)), 4)
			nf4_2 = round((len(sf4_2.index)) / (len(historical_df.index)), 4)
			nf4_3 = round((len(sf4_3.index)) / (len(historical_df.index)), 4)
			nf4_4 = round((len(sf4_4.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf4_4
			underestimating_rate = underestimating_rate + nf4_1 + nf4_2 + nf4_3

			df.loc['<2 years (sim)', '>=10 years (obs)'] = nf4_1
			df.loc['2 years - 5 years (sim)', '>=10 years (obs)'] = nf4_2
			df.loc['5 years - 10 years (sim)', '>=10 years (obs)'] = nf4_3
			df.loc['>=10 years (sim)', '>=10 years (obs)'] = nf4_4
			df.loc['Total (sim)', '>=10 years (obs)'] = nf4_1 + nf4_2 + nf4_3 + nf4_4

		# Finishing filling matrix
		df.loc['<2 years (sim)', 'Total (obs)'] = df.loc['<2 years (sim)', '<2 years (obs)'] + df.loc['<2 years (sim)', '2 years - 5 years (obs)'] + df.loc['<2 years (sim)', '5 years - 10 years (obs)'] + df.loc['<2 years (sim)', '>=10 years (obs)']
		df.loc['2 years - 5 years (sim)', 'Total (obs)'] = df.loc['2 years - 5 years (sim)', '<2 years (obs)'] + df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] + df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] + df.loc['2 years - 5 years (sim)', '>=10 years (obs)']
		df.loc['5 years - 10 years (sim)', 'Total (obs)'] = df.loc['5 years - 10 years (sim)', '<2 years (obs)'] + df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] + df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] + df.loc['5 years - 10 years (sim)', '>=10 years (obs)']
		df.loc['>=10 years (sim)', 'Total (obs)'] = df.loc['>=10 years (sim)', '<2 years (obs)'] + df.loc['>=10 years (sim)', '2 years - 5 years (obs)'] + df.loc['>=10 years (sim)', '5 years - 10 years (obs)'] + df.loc['>=10 years (sim)', '>=10 years (obs)']
		df.loc['Total (sim)', 'Total (obs)'] = df.loc['Total (sim)', '<2 years (obs)'] + df.loc['Total (sim)', '2 years - 5 years (obs)'] + df.loc['Total (sim)', '5 years - 10 years (obs)'] + df.loc['Total (sim)', '>=10 years (obs)']

		success_rate_ini = success_rate
		overestimating_rate_ini = overestimating_rate
		underestimating_rate_ini = underestimating_rate

		#print(df)
		#print(' ')
		#print('El porcentaje deéxito para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((success_rate * 100), 2)) + '%')
		#print('El porcentaje desobre-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((overestimating_rate * 100), 2)) + '%')
		#print('El porcentaje desub-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((underestimating_rate * 100), 2)) + '%')
		#print(' ')
		#print(' ')

	elif event_return_period == 25:

		data = {'<2 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '2 years - 5 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '5 years - 10 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '10 years - 25 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '>=25 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], 'Total (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000]}
		df = pd.DataFrame(data, index=['<2 years (sim)', '2 years - 5 years (sim)', '5 years - 10 years (sim)', '10 years - 25 years (sim)', '>=25 years (sim)', 'Total (sim)'])

		# Percentages
		success_rate = 0
		overestimating_rate = 0
		underestimating_rate = 0

		df1 = historical_df.loc[(historical_df['Observed'] < obs_2_threshold)]
		merged_df1 = pd.DataFrame.join(simulated_df, df1).dropna()
		merged_df1.drop('Observed', axis=1, inplace=True)

		if len(merged_df1.index) == 0:
			print('')
		else:
			sf1_1 = merged_df1.loc[(merged_df1['Simulated'] < sim_2_threshold)]
			sf1_2 = merged_df1.loc[(merged_df1['Simulated'] >= sim_2_threshold) & (merged_df1['Simulated'] < sim_5_threshold)]
			sf1_3 = merged_df1.loc[(merged_df1['Simulated'] >= sim_5_threshold) & (merged_df1['Simulated'] < sim_10_threshold)]
			sf1_4 = merged_df1.loc[(merged_df1['Simulated'] >= sim_10_threshold) & (merged_df1['Simulated'] < sim_25_threshold)]
			sf1_5 = merged_df1.loc[(merged_df1['Simulated'] >= sim_25_threshold)]

			nf1_1 = round((len(sf1_1.index)) / (len(historical_df.index)), 4)
			nf1_2 = round((len(sf1_2.index)) / (len(historical_df.index)), 4)
			nf1_3 = round((len(sf1_3.index)) / (len(historical_df.index)), 4)
			nf1_4 = round((len(sf1_4.index)) / (len(historical_df.index)), 4)
			nf1_5 = round((len(sf1_5.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf1_1
			overestimating_rate = overestimating_rate + nf1_2 + nf1_3 + nf1_4 + nf1_5

			df.loc['<2 years (sim)', '<2 years (obs)'] = nf1_1
			df.loc['2 years - 5 years (sim)', '<2 years (obs)'] = nf1_2
			df.loc['5 years - 10 years (sim)', '<2 years (obs)'] = nf1_3
			df.loc['10 years - 25 years (sim)', '<2 years (obs)'] = nf1_4
			df.loc['>=25 years (sim)', '<2 years (obs)'] = nf1_5
			df.loc['Total (sim)', '<2 years (obs)'] = nf1_1 + nf1_2 + nf1_3 + nf1_4 + nf1_5

		df2 = historical_df.loc[(historical_df['Observed'] >= obs_2_threshold) & (historical_df['Observed'] < obs_5_threshold)]
		merged_df2 = pd.DataFrame.join(simulated_df, df2).dropna()
		merged_df2.drop('Observed', axis=1, inplace=True)

		if len(merged_df2.index) == 0:
			print('')
		else:
			sf2_1 = merged_df2.loc[(merged_df2['Simulated'] < sim_2_threshold)]
			sf2_2 = merged_df2.loc[(merged_df2['Simulated'] >= sim_2_threshold) & (merged_df2['Simulated'] < sim_5_threshold)]
			sf2_3 = merged_df2.loc[(merged_df2['Simulated'] >= sim_5_threshold) & (merged_df2['Simulated'] < sim_10_threshold)]
			sf2_4 = merged_df2.loc[(merged_df2['Simulated'] >= sim_10_threshold) & (merged_df2['Simulated'] < sim_25_threshold)]
			sf2_5 = merged_df2.loc[(merged_df2['Simulated'] >= sim_25_threshold)]

			nf2_1 = round((len(sf2_1.index)) / (len(historical_df.index)), 4)
			nf2_2 = round((len(sf2_2.index)) / (len(historical_df.index)), 4)
			nf2_3 = round((len(sf2_3.index)) / (len(historical_df.index)), 4)
			nf2_4 = round((len(sf2_4.index)) / (len(historical_df.index)), 4)
			nf2_5 = round((len(sf2_5.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf2_2
			overestimating_rate = overestimating_rate + nf2_3 + nf2_4 + nf2_5
			underestimating_rate = underestimating_rate + nf2_1

			df.loc['<2 years (sim)', '2 years - 5 years (obs)'] = nf2_1
			df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] = nf2_2
			df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] = nf2_3
			df.loc['10 years - 25 years (sim)', '2 years - 5 years (obs)'] = nf2_4
			df.loc['>=25 years (sim)', '2 years - 5 years (obs)'] = nf2_5
			df.loc['Total (sim)', '2 years - 5 years (obs)'] = nf2_1 + nf2_2 + nf2_3 + nf2_4 + nf2_5

		df3 = historical_df.loc[(historical_df['Observed'] >= obs_5_threshold) & (historical_df['Observed'] < obs_10_threshold)]
		merged_df3 = pd.DataFrame.join(simulated_df, df3).dropna()
		merged_df3.drop('Observed', axis=1, inplace=True)

		if len(merged_df3.index) == 0:
			print('')
		else:
			sf3_1 = merged_df3.loc[(merged_df3['Simulated'] < sim_2_threshold)]
			sf3_2 = merged_df3.loc[(merged_df3['Simulated'] >= sim_2_threshold) & (merged_df3['Simulated'] < sim_5_threshold)]
			sf3_3 = merged_df3.loc[(merged_df3['Simulated'] >= sim_5_threshold) & (merged_df3['Simulated'] < sim_10_threshold)]
			sf3_4 = merged_df3.loc[(merged_df3['Simulated'] >= sim_10_threshold) & (merged_df3['Simulated'] < sim_25_threshold)]
			sf3_5 = merged_df3.loc[(merged_df3['Simulated'] >= sim_25_threshold)]

			nf3_1 = round((len(sf3_1.index)) / (len(historical_df.index)), 4)
			nf3_2 = round((len(sf3_2.index)) / (len(historical_df.index)), 4)
			nf3_3 = round((len(sf3_3.index)) / (len(historical_df.index)), 4)
			nf3_4 = round((len(sf3_4.index)) / (len(historical_df.index)), 4)
			nf3_5 = round((len(sf3_5.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf3_3
			overestimating_rate = overestimating_rate + nf3_4 + nf3_5
			underestimating_rate = underestimating_rate + nf3_1 + nf3_2

			df.loc['<2 years (sim)', '5 years - 10 years (obs)'] = nf3_1
			df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] = nf3_2
			df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] = nf3_3
			df.loc['10 years - 25 years (sim)', '5 years - 10 years (obs)'] = nf3_4
			df.loc['>=25 years (sim)', '5 years - 10 years (obs)'] = nf3_5
			df.loc['Total (sim)', '5 years - 10 years (obs)'] = nf3_1 + nf3_2 + nf3_3 + nf3_4 + nf3_5

		df4 = historical_df.loc[(historical_df['Observed'] >= obs_10_threshold) & (historical_df['Observed'] < obs_25_threshold)]
		merged_df4 = pd.DataFrame.join(simulated_df, df4).dropna()
		merged_df4.drop('Observed', axis=1, inplace=True)

		if len(merged_df4.index) == 0:
			print('')
		else:
			sf4_1 = merged_df4.loc[(merged_df4['Simulated'] < sim_2_threshold)]
			sf4_2 = merged_df4.loc[(merged_df4['Simulated'] >= sim_2_threshold) & (merged_df4['Simulated'] < sim_5_threshold)]
			sf4_3 = merged_df4.loc[(merged_df4['Simulated'] >= sim_5_threshold) & (merged_df4['Simulated'] < sim_10_threshold)]
			sf4_4 = merged_df4.loc[(merged_df4['Simulated'] >= sim_10_threshold) & (merged_df4['Simulated'] < sim_25_threshold)]
			sf4_5 = merged_df4.loc[(merged_df4['Simulated'] >= sim_25_threshold)]

			nf4_1 = round((len(sf4_1.index)) / (len(historical_df.index)), 4)
			nf4_2 = round((len(sf4_2.index)) / (len(historical_df.index)), 4)
			nf4_3 = round((len(sf4_3.index)) / (len(historical_df.index)), 4)
			nf4_4 = round((len(sf4_4.index)) / (len(historical_df.index)), 4)
			nf4_5 = round((len(sf4_5.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf4_4
			overestimating_rate = overestimating_rate + nf4_5
			underestimating_rate = underestimating_rate + nf4_1 + nf4_2 + nf4_3

			df.loc['<2 years (sim)', '10 years - 25 years (obs)'] = nf4_1
			df.loc['2 years - 5 years (sim)', '10 years - 25 years (obs)'] = nf4_2
			df.loc['5 years - 10 years (sim)', '10 years - 25 years (obs)'] = nf4_3
			df.loc['10 years - 25 years (sim)', '10 years - 25 years (obs)'] = nf4_4
			df.loc['>=25 years (sim)', '10 years - 25 years (obs)'] = nf4_5
			df.loc['Total (sim)', '10 years - 25 years (obs)'] = nf4_1 + nf4_2 + nf4_3 + nf4_4 + nf4_5

		df5 = historical_df.loc[(historical_df['Observed'] >= obs_25_threshold)]
		merged_df5 = pd.DataFrame.join(simulated_df, df5).dropna()
		merged_df5.drop('Observed', axis=1, inplace=True)

		if len(merged_df5.index) == 0:
			print('')
		else:
			sf5_1 = merged_df5.loc[(merged_df5['Simulated'] < sim_2_threshold)]
			sf5_2 = merged_df5.loc[(merged_df5['Simulated'] >= sim_2_threshold) & (merged_df5['Simulated'] < sim_5_threshold)]
			sf5_3 = merged_df5.loc[(merged_df5['Simulated'] >= sim_5_threshold) & (merged_df5['Simulated'] < sim_10_threshold)]
			sf5_4 = merged_df5.loc[(merged_df5['Simulated'] >= sim_10_threshold) & (merged_df5['Simulated'] < sim_25_threshold)]
			sf5_5 = merged_df5.loc[(merged_df5['Simulated'] >= sim_25_threshold)]

			nf5_1 = round((len(sf5_1.index)) / (len(historical_df.index)), 4)
			nf5_2 = round((len(sf5_2.index)) / (len(historical_df.index)), 4)
			nf5_3 = round((len(sf5_3.index)) / (len(historical_df.index)), 4)
			nf5_4 = round((len(sf5_4.index)) / (len(historical_df.index)), 4)
			nf5_5 = round((len(sf5_5.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf5_5
			underestimating_rate = underestimating_rate + nf5_1 + nf5_2 + nf5_3 + nf5_4

			df.loc['<2 years (sim)', '>=25 years (obs)'] = nf5_1
			df.loc['2 years - 5 years (sim)', '>=25 years (obs)'] = nf5_2
			df.loc['5 years - 10 years (sim)', '>=25 years (obs)'] = nf5_3
			df.loc['10 years - 25 years (sim)', '>=25 years (obs)'] = nf5_4
			df.loc['>=25 years (sim)', '>=25 years (obs)'] = nf5_5
			df.loc['Total (sim)', '>=25 years (obs)'] = nf5_1 + nf5_2 + nf5_3 + nf5_4 + nf5_5

		# Finishing filling matrix
		df.loc['<2 years (sim)', 'Total (obs)'] = df.loc['<2 years (sim)', '<2 years (obs)'] + df.loc['<2 years (sim)', '2 years - 5 years (obs)'] + df.loc['<2 years (sim)', '5 years - 10 years (obs)'] + df.loc['<2 years (sim)', '10 years - 25 years (obs)'] + df.loc['<2 years (sim)', '>=25 years (obs)']
		df.loc['2 years - 5 years (sim)', 'Total (obs)'] = df.loc['2 years - 5 years (sim)', '<2 years (obs)'] + df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] + df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] + df.loc['2 years - 5 years (sim)', '10 years - 25 years (obs)'] + df.loc['2 years - 5 years (sim)', '>=25 years (obs)']
		df.loc['5 years - 10 years (sim)', 'Total (obs)'] = df.loc['5 years - 10 years (sim)', '<2 years (obs)'] + df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] + df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] + df.loc['5 years - 10 years (sim)', '10 years - 25 years (obs)'] + df.loc['5 years - 10 years (sim)', '>=25 years (obs)']
		df.loc['10 years - 25 years (sim)', 'Total (obs)'] = df.loc['10 years - 25 years (sim)', '<2 years (obs)'] + df.loc['10 years - 25 years (sim)', '2 years - 5 years (obs)'] + df.loc['10 years - 25 years (sim)', '5 years - 10 years (obs)'] + df.loc['10 years - 25 years (sim)', '10 years - 25 years (obs)'] + df.loc['10 years - 25 years (sim)', '>=25 years (obs)']
		df.loc['>=25 years (sim)', 'Total (obs)'] = df.loc['>=25 years (sim)', '<2 years (obs)'] + df.loc['>=25 years (sim)', '2 years - 5 years (obs)'] + df.loc['>=25 years (sim)', '5 years - 10 years (obs)'] + df.loc['>=25 years (sim)', '10 years - 25 years (obs)'] + df.loc['>=25 years (sim)', '>=25 years (obs)']
		df.loc['Total (sim)', 'Total (obs)'] = df.loc['Total (sim)', '<2 years (obs)'] + df.loc['Total (sim)', '2 years - 5 years (obs)'] + df.loc['Total (sim)', '5 years - 10 years (obs)'] + df.loc['Total (sim)', '10 years - 25 years (obs)'] + df.loc['Total (sim)', '>=25 years (obs)']

		success_rate_ini = success_rate
		overestimating_rate_ini = overestimating_rate
		underestimating_rate_ini = underestimating_rate

		#print(df)
		#print(' ')
		#print('El porcentaje deéxito para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((success_rate * 100), 2)) + '%')
		#print('El porcentaje desobre-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((overestimating_rate * 100), 2)) + '%')
		#print('El porcentaje desub-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((underestimating_rate * 100), 2)) + '%')
		#print(' ')
		#print(' ')

	elif event_return_period == 50:

		data = {'<2 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '2 years - 5 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '5 years - 10 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '10 years - 25 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '25 years - 50 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '>=50 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], 'Total (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000]}
		df = pd.DataFrame(data, index=['<2 years (sim)', '2 years - 5 years (sim)', '5 years - 10 years (sim)','10 years - 25 years (sim)', '25 years - 50 years (sim)', '>=50 years (sim)', 'Total (sim)'])

		# Percentages
		success_rate = 0
		overestimating_rate = 0
		underestimating_rate = 0

		df1 = historical_df.loc[(historical_df['Observed'] < obs_2_threshold)]
		merged_df1 = pd.DataFrame.join(simulated_df, df1).dropna()
		merged_df1.drop('Observed', axis=1, inplace=True)

		if len(merged_df1.index) == 0:
			print('')
		else:
			sf1_1 = merged_df1.loc[(merged_df1['Simulated'] < sim_2_threshold)]
			sf1_2 = merged_df1.loc[(merged_df1['Simulated'] >= sim_2_threshold) & (merged_df1['Simulated'] < sim_5_threshold)]
			sf1_3 = merged_df1.loc[(merged_df1['Simulated'] >= sim_5_threshold) & (merged_df1['Simulated'] < sim_10_threshold)]
			sf1_4 = merged_df1.loc[(merged_df1['Simulated'] >= sim_10_threshold) & (merged_df1['Simulated'] < sim_25_threshold)]
			sf1_5 = merged_df1.loc[(merged_df1['Simulated'] >= sim_25_threshold) & (merged_df1['Simulated'] < sim_50_threshold)]
			sf1_6 = merged_df1.loc[(merged_df1['Simulated'] >= sim_50_threshold)]

			nf1_1 = round((len(sf1_1.index)) / (len(historical_df.index)), 4)
			nf1_2 = round((len(sf1_2.index)) / (len(historical_df.index)), 4)
			nf1_3 = round((len(sf1_3.index)) / (len(historical_df.index)), 4)
			nf1_4 = round((len(sf1_4.index)) / (len(historical_df.index)), 4)
			nf1_5 = round((len(sf1_5.index)) / (len(historical_df.index)), 4)
			nf1_6 = round((len(sf1_6.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf1_1
			overestimating_rate = overestimating_rate + nf1_2 + nf1_3 + nf1_4 + nf1_5 + nf1_6

			df.loc['<2 years (sim)', '<2 years (obs)'] = nf1_1
			df.loc['2 years - 5 years (sim)', '<2 years (obs)'] = nf1_2
			df.loc['5 years - 10 years (sim)', '<2 years (obs)'] = nf1_3
			df.loc['10 years - 25 years (sim)', '<2 years (obs)'] = nf1_4
			df.loc['25 years - 50 years (sim)', '<2 years (obs)'] = nf1_5
			df.loc['>=50 years (sim)', '<2 years (obs)'] = nf1_6
			df.loc['Total (sim)', '<2 years (obs)'] = nf1_1 + nf1_2 + nf1_3 + nf1_4 + nf1_5 + nf1_6

		df2 = historical_df.loc[(historical_df['Observed'] >= obs_2_threshold) & (historical_df['Observed'] < obs_5_threshold)]
		merged_df2 = pd.DataFrame.join(simulated_df, df2).dropna()
		merged_df2.drop('Observed', axis=1, inplace=True)

		if len(merged_df2.index) == 0:
			print('')
		else:
			sf2_1 = merged_df2.loc[(merged_df2['Simulated'] < sim_2_threshold)]
			sf2_2 = merged_df2.loc[(merged_df2['Simulated'] >= sim_2_threshold) & (merged_df2['Simulated'] < sim_5_threshold)]
			sf2_3 = merged_df2.loc[(merged_df2['Simulated'] >= sim_5_threshold) & (merged_df2['Simulated'] < sim_10_threshold)]
			sf2_4 = merged_df2.loc[(merged_df2['Simulated'] >= sim_10_threshold) & (merged_df2['Simulated'] < sim_25_threshold)]
			sf2_5 = merged_df2.loc[(merged_df2['Simulated'] >= sim_25_threshold) & (merged_df2['Simulated'] < sim_50_threshold)]
			sf2_6 = merged_df2.loc[(merged_df2['Simulated'] >= sim_50_threshold)]

			nf2_1 = round((len(sf2_1.index)) / (len(historical_df.index)), 4)
			nf2_2 = round((len(sf2_2.index)) / (len(historical_df.index)), 4)
			nf2_3 = round((len(sf2_3.index)) / (len(historical_df.index)), 4)
			nf2_4 = round((len(sf2_4.index)) / (len(historical_df.index)), 4)
			nf2_5 = round((len(sf2_5.index)) / (len(historical_df.index)), 4)
			nf2_6 = round((len(sf2_6.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf2_2
			overestimating_rate = overestimating_rate + nf2_3 + nf2_4 + nf2_5 + nf2_6
			underestimating_rate = underestimating_rate + nf2_1

			df.loc['<2 years (sim)', '2 years - 5 years (obs)'] = nf2_1
			df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] = nf2_2
			df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] = nf2_3
			df.loc['10 years - 25 years (sim)', '2 years - 5 years (obs)'] = nf2_4
			df.loc['25 years - 50 years (sim)', '2 years - 5 years (obs)'] = nf2_5
			df.loc['>=50 years (sim)', '2 years - 5 years (obs)'] = nf2_6
			df.loc['Total (sim)', '2 years - 5 years (obs)'] = nf2_1 + nf2_2 + nf2_3 + nf2_4 + nf2_5 + nf2_6

		df3 = historical_df.loc[(historical_df['Observed'] >= obs_5_threshold) & (historical_df['Observed'] < obs_10_threshold)]
		merged_df3 = pd.DataFrame.join(simulated_df, df3).dropna()
		merged_df3.drop('Observed', axis=1, inplace=True)

		if len(merged_df3.index) == 0:
			print('')
		else:
			sf3_1 = merged_df3.loc[(merged_df3['Simulated'] < sim_2_threshold)]
			sf3_2 = merged_df3.loc[(merged_df3['Simulated'] >= sim_2_threshold) & (merged_df3['Simulated'] < sim_5_threshold)]
			sf3_3 = merged_df3.loc[(merged_df3['Simulated'] >= sim_5_threshold) & (merged_df3['Simulated'] < sim_10_threshold)]
			sf3_4 = merged_df3.loc[(merged_df3['Simulated'] >= sim_10_threshold) & (merged_df3['Simulated'] < sim_25_threshold)]
			sf3_5 = merged_df3.loc[(merged_df3['Simulated'] >= sim_25_threshold) & (merged_df3['Simulated'] < sim_50_threshold)]
			sf3_6 = merged_df3.loc[(merged_df3['Simulated'] >= sim_50_threshold)]

			nf3_1 = round((len(sf3_1.index)) / (len(historical_df.index)), 4)
			nf3_2 = round((len(sf3_2.index)) / (len(historical_df.index)), 4)
			nf3_3 = round((len(sf3_3.index)) / (len(historical_df.index)), 4)
			nf3_4 = round((len(sf3_4.index)) / (len(historical_df.index)), 4)
			nf3_5 = round((len(sf3_5.index)) / (len(historical_df.index)), 4)
			nf3_6 = round((len(sf3_6.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf3_3
			overestimating_rate = overestimating_rate + nf3_4 + nf3_5 + nf3_6
			underestimating_rate = underestimating_rate + nf3_1 + nf3_2

			df.loc['<2 years (sim)', '5 years - 10 years (obs)'] = nf3_1
			df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] = nf3_2
			df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] = nf3_3
			df.loc['10 years - 25 years (sim)', '5 years - 10 years (obs)'] = nf3_4
			df.loc['25 years - 50 years (sim)', '5 years - 10 years (obs)'] = nf3_5
			df.loc['>=50 years (sim)', '5 years - 10 years (obs)'] = nf3_6
			df.loc['Total (sim)', '5 years - 10 years (obs)'] = nf3_1 + nf3_2 + nf3_3 + nf3_4 + nf3_5 + nf3_6

		df4 = historical_df.loc[(historical_df['Observed'] >= obs_10_threshold) & (historical_df['Observed'] < obs_25_threshold)]
		merged_df4 = pd.DataFrame.join(simulated_df, df4).dropna()
		merged_df4.drop('Observed', axis=1, inplace=True)

		if len(merged_df4.index) == 0:
			print('')
		else:
			sf4_1 = merged_df4.loc[(merged_df4['Simulated'] < sim_2_threshold)]
			sf4_2 = merged_df4.loc[(merged_df4['Simulated'] >= sim_2_threshold) & (merged_df4['Simulated'] < sim_5_threshold)]
			sf4_3 = merged_df4.loc[(merged_df4['Simulated'] >= sim_5_threshold) & (merged_df4['Simulated'] < sim_10_threshold)]
			sf4_4 = merged_df4.loc[(merged_df4['Simulated'] >= sim_10_threshold) & (merged_df4['Simulated'] < sim_25_threshold)]
			sf4_5 = merged_df4.loc[(merged_df4['Simulated'] >= sim_25_threshold) & (merged_df4['Simulated'] < sim_50_threshold)]
			sf4_6 = merged_df4.loc[(merged_df4['Simulated'] >= sim_50_threshold)]

			nf4_1 = round((len(sf4_1.index)) / (len(historical_df.index)), 4)
			nf4_2 = round((len(sf4_2.index)) / (len(historical_df.index)), 4)
			nf4_3 = round((len(sf4_3.index)) / (len(historical_df.index)), 4)
			nf4_4 = round((len(sf4_4.index)) / (len(historical_df.index)), 4)
			nf4_5 = round((len(sf4_5.index)) / (len(historical_df.index)), 4)
			nf4_6 = round((len(sf4_6.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf4_4
			overestimating_rate = overestimating_rate + nf4_5 + nf4_6
			underestimating_rate = underestimating_rate + nf4_1 + nf4_2 + nf4_3

			df.loc['<2 years (sim)', '10 years - 25 years (obs)'] = nf4_1
			df.loc['2 years - 5 years (sim)', '10 years - 25 years (obs)'] = nf4_2
			df.loc['5 years - 10 years (sim)', '10 years - 25 years (obs)'] = nf4_3
			df.loc['10 years - 25 years (sim)', '10 years - 25 years (obs)'] = nf4_4
			df.loc['25 years - 50 years (sim)', '10 years - 25 years (obs)'] = nf4_5
			df.loc['>=50 years (sim)', '10 years - 25 years (obs)'] = nf4_6
			df.loc['Total (sim)', '10 years - 25 years (obs)'] = nf4_1 + nf4_2 + nf4_3 + nf4_4 + nf4_5 + nf4_6

		df5 = historical_df.loc[(historical_df['Observed'] >= obs_25_threshold) & (historical_df['Observed'] < obs_50_threshold)]
		merged_df5 = pd.DataFrame.join(simulated_df, df5).dropna()
		merged_df5.drop('Observed', axis=1, inplace=True)

		if len(merged_df5.index) == 0:
			print('')
		else:
			sf5_1 = merged_df5.loc[(merged_df5['Simulated'] < sim_2_threshold)]
			sf5_2 = merged_df5.loc[(merged_df5['Simulated'] >= sim_2_threshold) & (merged_df5['Simulated'] < sim_5_threshold)]
			sf5_3 = merged_df5.loc[(merged_df5['Simulated'] >= sim_5_threshold) & (merged_df5['Simulated'] < sim_10_threshold)]
			sf5_4 = merged_df5.loc[(merged_df5['Simulated'] >= sim_10_threshold) & (merged_df5['Simulated'] < sim_25_threshold)]
			sf5_5 = merged_df5.loc[(merged_df5['Simulated'] >= sim_25_threshold) & (merged_df5['Simulated'] < sim_50_threshold)]
			sf5_6 = merged_df5.loc[(merged_df5['Simulated'] >= sim_50_threshold)]

			nf5_1 = round((len(sf5_1.index)) / (len(historical_df.index)), 4)
			nf5_2 = round((len(sf5_2.index)) / (len(historical_df.index)), 4)
			nf5_3 = round((len(sf5_3.index)) / (len(historical_df.index)), 4)
			nf5_4 = round((len(sf5_4.index)) / (len(historical_df.index)), 4)
			nf5_5 = round((len(sf5_5.index)) / (len(historical_df.index)), 4)
			nf5_6 = round((len(sf5_6.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf5_5
			overestimating_rate = overestimating_rate + nf5_6
			underestimating_rate = underestimating_rate + nf5_1 + nf5_2 + nf5_3 + nf5_4

			df.loc['<2 years (sim)', '25 years - 50 years (obs)'] = nf5_1
			df.loc['2 years - 5 years (sim)', '25 years - 50 years (obs)'] = nf5_2
			df.loc['5 years - 10 years (sim)', '25 years - 50 years (obs)'] = nf5_3
			df.loc['10 years - 25 years (sim)', '25 years - 50 years (obs)'] = nf5_4
			df.loc['25 years - 50 years (sim)', '25 years - 50 years (obs)'] = nf5_5
			df.loc['>=50 years (sim)', '25 years - 50 years (obs)'] = nf5_6
			df.loc['Total (sim)', '25 years - 50 years (obs)'] = nf5_1 + nf5_2 + nf5_3 + nf5_4 + nf5_5 + nf5_6

		df6 = historical_df.loc[(historical_df['Observed'] >= obs_50_threshold)]
		merged_df6 = pd.DataFrame.join(simulated_df, df6).dropna()
		merged_df6.drop('Observed', axis=1, inplace=True)

		if len(merged_df6.index) == 0:
			print('')
		else:
			sf6_1 = merged_df6.loc[(merged_df6['Simulated'] < sim_2_threshold)]
			sf6_2 = merged_df6.loc[(merged_df6['Simulated'] >= sim_2_threshold) & (merged_df6['Simulated'] < sim_5_threshold)]
			sf6_3 = merged_df6.loc[(merged_df6['Simulated'] >= sim_5_threshold) & (merged_df6['Simulated'] < sim_10_threshold)]
			sf6_4 = merged_df6.loc[(merged_df6['Simulated'] >= sim_10_threshold) & (merged_df6['Simulated'] < sim_25_threshold)]
			sf6_5 = merged_df6.loc[(merged_df6['Simulated'] >= sim_25_threshold) & (merged_df6['Simulated'] < sim_50_threshold)]
			sf6_6 = merged_df6.loc[(merged_df6['Simulated'] >= sim_50_threshold)]

			nf6_1 = round((len(sf6_1.index)) / (len(historical_df.index)), 4)
			nf6_2 = round((len(sf6_2.index)) / (len(historical_df.index)), 4)
			nf6_3 = round((len(sf6_3.index)) / (len(historical_df.index)), 4)
			nf6_4 = round((len(sf6_4.index)) / (len(historical_df.index)), 4)
			nf6_5 = round((len(sf6_5.index)) / (len(historical_df.index)), 4)
			nf6_6 = round((len(sf6_6.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf6_6
			underestimating_rate = underestimating_rate + nf6_1 + nf6_2 + nf6_3 + nf6_4 + nf6_5

			df.loc['<2 years (sim)', '>=50 years (obs)'] = nf6_1
			df.loc['2 years - 5 years (sim)', '>=50 years (obs)'] = nf6_2
			df.loc['5 years - 10 years (sim)', '>=50 years (obs)'] = nf6_3
			df.loc['10 years - 25 years (sim)', '>=50 years (obs)'] = nf6_4
			df.loc['25 years - 50 years (sim)', '>=50 years (obs)'] = nf6_5
			df.loc['>=50 years (sim)', '>=50 years (obs)'] = nf6_6
			df.loc['Total (sim)', '>=50 years (obs)'] = nf6_1 + nf6_2 + nf6_3 + nf6_4 + nf6_5 + nf6_6

		# Finishing filling matrix
		df.loc['<2 years (sim)', 'Total (obs)'] = df.loc['<2 years (sim)', '<2 years (obs)'] + df.loc['<2 years (sim)', '2 years - 5 years (obs)'] + df.loc['<2 years (sim)', '5 years - 10 years (obs)'] + df.loc['<2 years (sim)', '10 years - 25 years (obs)'] + df.loc['<2 years (sim)', '25 years - 50 years (obs)'] + df.loc['<2 years (sim)', '>=50 years (obs)']
		df.loc['2 years - 5 years (sim)', 'Total (obs)'] = df.loc['2 years - 5 years (sim)', '<2 years (obs)'] + df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] + df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] + df.loc['2 years - 5 years (sim)', '10 years - 25 years (obs)'] + df.loc['2 years - 5 years (sim)', '25 years - 50 years (obs)'] + df.loc['2 years - 5 years (sim)', '>=50 years (obs)']
		df.loc['5 years - 10 years (sim)', 'Total (obs)'] = df.loc['5 years - 10 years (sim)', '<2 years (obs)'] + df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] + df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] + df.loc['5 years - 10 years (sim)', '10 years - 25 years (obs)'] + df.loc['5 years - 10 years (sim)', '25 years - 50 years (obs)'] + df.loc['5 years - 10 years (sim)', '>=50 years (obs)']
		df.loc['10 years - 25 years (sim)', 'Total (obs)'] = df.loc['10 years - 25 years (sim)', '<2 years (obs)'] + df.loc['10 years - 25 years (sim)', '2 years - 5 years (obs)'] + df.loc['10 years - 25 years (sim)', '5 years - 10 years (obs)'] + df.loc['10 years - 25 years (sim)', '10 years - 25 years (obs)'] + df.loc['10 years - 25 years (sim)', '25 years - 50 years (obs)'] + df.loc['10 years - 25 years (sim)', '>=50 years (obs)']
		df.loc['25 years - 50 years (sim)', 'Total (obs)'] = df.loc['25 years - 50 years (sim)', '<2 years (obs)'] + df.loc['25 years - 50 years (sim)', '2 years - 5 years (obs)'] + df.loc['25 years - 50 years (sim)', '5 years - 10 years (obs)'] + df.loc['25 years - 50 years (sim)', '10 years - 25 years (obs)'] + df.loc['25 years - 50 years (sim)', '25 years - 50 years (obs)'] + df.loc['25 years - 50 years (sim)', '>=50 years (obs)']
		df.loc['>=50 years (sim)', 'Total (obs)'] = df.loc['>=50 years (sim)', '<2 years (obs)'] + df.loc['>=50 years (sim)', '2 years - 5 years (obs)'] + df.loc['>=50 years (sim)', '5 years - 10 years (obs)'] + df.loc['>=50 years (sim)', '10 years - 25 years (obs)'] + df.loc['>=50 years (sim)', '25 years - 50 years (obs)'] + df.loc['>=50 years (sim)', '>=50 years (obs)']
		df.loc['Total (sim)', 'Total (obs)'] = df.loc['Total (sim)', '<2 years (obs)'] + df.loc['Total (sim)', '2 years - 5 years (obs)'] + df.loc['Total (sim)', '5 years - 10 years (obs)'] + df.loc['Total (sim)', '10 years - 25 years (obs)'] + df.loc['Total (sim)', '25 years - 50 years (obs)'] + df.loc['Total (sim)', '>=50 years (obs)']

		success_rate_ini = success_rate
		overestimating_rate_ini = overestimating_rate
		underestimating_rate_ini = underestimating_rate

		#print(df)
		#print(' ')
		#print('El porcentaje deéxito para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((success_rate * 100), 2)) + '%')
		#print('El porcentaje desobre-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((overestimating_rate * 100), 2)) + '%')
		#print('El porcentaje desub-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((underestimating_rate * 100), 2)) + '%')
		#print(' ')
		#print(' ')

	elif event_return_period == 100:

		data = {'<2 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '2 years - 5 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '5 years - 10 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '10 years - 25 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '25 years - 50 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '50 years - 100 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], '>=100 years (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000], 'Total (obs)': [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000]}
		df = pd.DataFrame(data, index=['<2 years (sim)', '2 years - 5 years (sim)', '5 years - 10 years (sim)', '10 years - 25 years (sim)', '25 years - 50 years (sim)', '50 years - 100 years (sim)', '>=100 years (sim)', 'Total (sim)'])

		# Percentages
		success_rate = 0
		overestimating_rate = 0
		underestimating_rate = 0

		df1 = historical_df.loc[(historical_df['Observed'] < obs_2_threshold)]
		merged_df1 = pd.DataFrame.join(simulated_df, df1).dropna()
		merged_df1.drop('Observed', axis=1, inplace=True)

		if len(merged_df1.index) == 0:
			print('')
		else:
			sf1_1 = merged_df1.loc[(merged_df1['Simulated'] < sim_2_threshold)]
			sf1_2 = merged_df1.loc[(merged_df1['Simulated'] >= sim_2_threshold) & (merged_df1['Simulated'] < sim_5_threshold)]
			sf1_3 = merged_df1.loc[(merged_df1['Simulated'] >= sim_5_threshold) & (merged_df1['Simulated'] < sim_10_threshold)]
			sf1_4 = merged_df1.loc[(merged_df1['Simulated'] >= sim_10_threshold) & (merged_df1['Simulated'] < sim_25_threshold)]
			sf1_5 = merged_df1.loc[(merged_df1['Simulated'] >= sim_25_threshold) & (merged_df1['Simulated'] < sim_50_threshold)]
			sf1_6 = merged_df1.loc[(merged_df1['Simulated'] >= sim_50_threshold) & (merged_df1['Simulated'] < sim_100_threshold)]
			sf1_7 = merged_df1.loc[(merged_df1['Simulated'] >= sim_100_threshold)]

			nf1_1 = round((len(sf1_1.index)) / (len(historical_df.index)), 4)
			nf1_2 = round((len(sf1_2.index)) / (len(historical_df.index)), 4)
			nf1_3 = round((len(sf1_3.index)) / (len(historical_df.index)), 4)
			nf1_4 = round((len(sf1_4.index)) / (len(historical_df.index)), 4)
			nf1_5 = round((len(sf1_5.index)) / (len(historical_df.index)), 4)
			nf1_6 = round((len(sf1_6.index)) / (len(historical_df.index)), 4)
			nf1_7 = round((len(sf1_7.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf1_1
			overestimating_rate = overestimating_rate + nf1_2 + nf1_3 + nf1_4 + nf1_5 + nf1_6 + nf1_7

			df.loc['<2 years (sim)', '<2 years (obs)'] = nf1_1
			df.loc['2 years - 5 years (sim)', '<2 years (obs)'] = nf1_2
			df.loc['5 years - 10 years (sim)', '<2 years (obs)'] = nf1_3
			df.loc['10 years - 25 years (sim)', '<2 years (obs)'] = nf1_4
			df.loc['25 years - 50 years (sim)', '<2 years (obs)'] = nf1_5
			df.loc['50 years - 100 years (sim)', '<2 years (obs)'] = nf1_6
			df.loc['>=100 years (sim)', '<2 years (obs)'] = nf1_7
			df.loc['Total (sim)', '<2 years (obs)'] = nf1_1 + nf1_2 + nf1_3 + nf1_4 + nf1_5 + nf1_6 + nf1_7

		df2 = historical_df.loc[(historical_df['Observed'] >= obs_2_threshold) & (historical_df['Observed'] < obs_5_threshold)]
		merged_df2 = pd.DataFrame.join(simulated_df, df2).dropna()
		merged_df2.drop('Observed', axis=1, inplace=True)

		if len(merged_df2.index) == 0:
			print('')
		else:
			sf2_1 = merged_df2.loc[(merged_df2['Simulated'] < sim_2_threshold)]
			sf2_2 = merged_df2.loc[(merged_df2['Simulated'] >= sim_2_threshold) & (merged_df2['Simulated'] < sim_5_threshold)]
			sf2_3 = merged_df2.loc[(merged_df2['Simulated'] >= sim_5_threshold) & (merged_df2['Simulated'] < sim_10_threshold)]
			sf2_4 = merged_df2.loc[(merged_df2['Simulated'] >= sim_10_threshold) & (merged_df2['Simulated'] < sim_25_threshold)]
			sf2_5 = merged_df2.loc[(merged_df2['Simulated'] >= sim_25_threshold) & (merged_df2['Simulated'] < sim_50_threshold)]
			sf2_6 = merged_df2.loc[(merged_df2['Simulated'] >= sim_50_threshold) & (merged_df2['Simulated'] < sim_100_threshold)]
			sf2_7 = merged_df2.loc[(merged_df2['Simulated'] >= sim_100_threshold)]

			nf2_1 = round((len(sf2_1.index)) / (len(historical_df.index)), 4)
			nf2_2 = round((len(sf2_2.index)) / (len(historical_df.index)), 4)
			nf2_3 = round((len(sf2_3.index)) / (len(historical_df.index)), 4)
			nf2_4 = round((len(sf2_4.index)) / (len(historical_df.index)), 4)
			nf2_5 = round((len(sf2_5.index)) / (len(historical_df.index)), 4)
			nf2_6 = round((len(sf2_6.index)) / (len(historical_df.index)), 4)
			nf2_7 = round((len(sf2_7.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf2_2
			overestimating_rate = overestimating_rate + nf2_3 + nf2_4 + nf2_5 + nf2_6 + nf2_7
			underestimating_rate = underestimating_rate + nf2_1

			df.loc['<2 years (sim)', '2 years - 5 years (obs)'] = nf2_1
			df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] = nf2_2
			df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] = nf2_3
			df.loc['10 years - 25 years (sim)', '2 years - 5 years (obs)'] = nf2_4
			df.loc['25 years - 50 years (sim)', '2 years - 5 years (obs)'] = nf2_5
			df.loc['50 years - 100 years (sim)', '2 years - 5 years (obs)'] = nf2_6
			df.loc['>=100 years (sim)', '2 years - 5 years (obs)'] = nf2_7
			df.loc['Total (sim)', '2 years - 5 years (obs)'] = nf2_1 + nf2_2 + nf2_3 + nf2_4 + nf2_5 + nf2_6 + nf2_7

		df3 = historical_df.loc[(historical_df['Observed'] >= obs_5_threshold) & (historical_df['Observed'] < obs_10_threshold)]
		merged_df3 = pd.DataFrame.join(simulated_df, df3).dropna()
		merged_df3.drop('Observed', axis=1, inplace=True)

		if len(merged_df3.index) == 0:
			print('')
		else:
			sf3_1 = merged_df3.loc[(merged_df3['Simulated'] < sim_2_threshold)]
			sf3_2 = merged_df3.loc[(merged_df3['Simulated'] >= sim_2_threshold) & (merged_df3['Simulated'] < sim_5_threshold)]
			sf3_3 = merged_df3.loc[(merged_df3['Simulated'] >= sim_5_threshold) & (merged_df3['Simulated'] < sim_10_threshold)]
			sf3_4 = merged_df3.loc[(merged_df3['Simulated'] >= sim_10_threshold) & (merged_df3['Simulated'] < sim_25_threshold)]
			sf3_5 = merged_df3.loc[(merged_df3['Simulated'] >= sim_25_threshold) & (merged_df3['Simulated'] < sim_50_threshold)]
			sf3_6 = merged_df3.loc[(merged_df3['Simulated'] >= sim_50_threshold) & (merged_df3['Simulated'] < sim_100_threshold)]
			sf3_7 = merged_df3.loc[(merged_df3['Simulated'] >= sim_100_threshold)]

			nf3_1 = round((len(sf3_1.index)) / (len(historical_df.index)), 4)
			nf3_2 = round((len(sf3_2.index)) / (len(historical_df.index)), 4)
			nf3_3 = round((len(sf3_3.index)) / (len(historical_df.index)), 4)
			nf3_4 = round((len(sf3_4.index)) / (len(historical_df.index)), 4)
			nf3_5 = round((len(sf3_5.index)) / (len(historical_df.index)), 4)
			nf3_6 = round((len(sf3_6.index)) / (len(historical_df.index)), 4)
			nf3_7 = round((len(sf3_7.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf3_3
			overestimating_rate = overestimating_rate + nf3_4 + nf3_5 + nf3_6 + nf3_7
			underestimating_rate = underestimating_rate + nf3_1 + nf3_2

			df.loc['<2 years (sim)', '5 years - 10 years (obs)'] = nf3_1
			df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] = nf3_2
			df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] = nf3_3
			df.loc['10 years - 25 years (sim)', '5 years - 10 years (obs)'] = nf3_4
			df.loc['25 years - 50 years (sim)', '5 years - 10 years (obs)'] = nf3_5
			df.loc['50 years - 100 years (sim)', '5 years - 10 years (obs)'] = nf3_6
			df.loc['>=100 years (sim)', '5 years - 10 years (obs)'] = nf3_7
			df.loc['Total (sim)', '5 years - 10 years (obs)'] = nf3_1 + nf3_2 + nf3_3 + nf3_4 + nf3_5 + nf3_6 + nf3_7

		df4 = historical_df.loc[(historical_df['Observed'] >= obs_10_threshold) & (historical_df['Observed'] < obs_25_threshold)]
		merged_df4 = pd.DataFrame.join(simulated_df, df4).dropna()
		merged_df4.drop('Observed', axis=1, inplace=True)

		if len(merged_df4.index) == 0:
			print('')
		else:
			sf4_1 = merged_df4.loc[(merged_df4['Simulated'] < sim_2_threshold)]
			sf4_2 = merged_df4.loc[(merged_df4['Simulated'] >= sim_2_threshold) & (merged_df4['Simulated'] < sim_5_threshold)]
			sf4_3 = merged_df4.loc[(merged_df4['Simulated'] >= sim_5_threshold) & (merged_df4['Simulated'] < sim_10_threshold)]
			sf4_4 = merged_df4.loc[(merged_df4['Simulated'] >= sim_10_threshold) & (merged_df4['Simulated'] < sim_25_threshold)]
			sf4_5 = merged_df4.loc[(merged_df4['Simulated'] >= sim_25_threshold) & (merged_df4['Simulated'] < sim_50_threshold)]
			sf4_6 = merged_df4.loc[(merged_df4['Simulated'] >= sim_50_threshold) & (merged_df4['Simulated'] < sim_100_threshold)]
			sf4_7 = merged_df4.loc[(merged_df4['Simulated'] >= sim_100_threshold)]

			nf4_1 = round((len(sf4_1.index)) / (len(historical_df.index)), 4)
			nf4_2 = round((len(sf4_2.index)) / (len(historical_df.index)), 4)
			nf4_3 = round((len(sf4_3.index)) / (len(historical_df.index)), 4)
			nf4_4 = round((len(sf4_4.index)) / (len(historical_df.index)), 4)
			nf4_5 = round((len(sf4_5.index)) / (len(historical_df.index)), 4)
			nf4_6 = round((len(sf4_6.index)) / (len(historical_df.index)), 4)
			nf4_7 = round((len(sf4_7.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf4_4
			overestimating_rate = overestimating_rate + nf4_5 + nf4_6 + nf4_7
			underestimating_rate = underestimating_rate + nf4_1 + nf4_2 + nf4_3

			df.loc['<2 years (sim)', '10 years - 25 years (obs)'] = nf4_1
			df.loc['2 years - 5 years (sim)', '10 years - 25 years (obs)'] = nf4_2
			df.loc['5 years - 10 years (sim)', '10 years - 25 years (obs)'] = nf4_3
			df.loc['10 years - 25 years (sim)', '10 years - 25 years (obs)'] = nf4_4
			df.loc['25 years - 50 years (sim)', '10 years - 25 years (obs)'] = nf4_5
			df.loc['50 years - 100 years (sim)', '10 years - 25 years (obs)'] = nf4_6
			df.loc['>=100 years (sim)', '10 years - 25 years (obs)'] = nf4_7
			df.loc['Total (sim)', '10 years - 25 years (obs)'] = nf4_1 + nf4_2 + nf4_3 + nf4_4 + nf4_5 + nf4_6 + nf4_7

		df5 = historical_df.loc[(historical_df['Observed'] >= obs_25_threshold) & (historical_df['Observed'] < obs_50_threshold)]
		merged_df5 = pd.DataFrame.join(simulated_df, df5).dropna()
		merged_df5.drop('Observed', axis=1, inplace=True)

		if len(merged_df5.index) == 0:
			print('')
		else:
			sf5_1 = merged_df5.loc[(merged_df5['Simulated'] < sim_2_threshold)]
			sf5_2 = merged_df5.loc[(merged_df5['Simulated'] >= sim_2_threshold) & (merged_df5['Simulated'] < sim_5_threshold)]
			sf5_3 = merged_df5.loc[(merged_df5['Simulated'] >= sim_5_threshold) & (merged_df5['Simulated'] < sim_10_threshold)]
			sf5_4 = merged_df5.loc[(merged_df5['Simulated'] >= sim_10_threshold) & (merged_df5['Simulated'] < sim_25_threshold)]
			sf5_5 = merged_df5.loc[(merged_df5['Simulated'] >= sim_25_threshold) & (merged_df5['Simulated'] < sim_50_threshold)]
			sf5_6 = merged_df5.loc[(merged_df5['Simulated'] >= sim_50_threshold) & (merged_df5['Simulated'] < sim_100_threshold)]
			sf5_7 = merged_df5.loc[(merged_df5['Simulated'] >= sim_100_threshold)]

			nf5_1 = round((len(sf5_1.index)) / (len(historical_df.index)), 4)
			nf5_2 = round((len(sf5_2.index)) / (len(historical_df.index)), 4)
			nf5_3 = round((len(sf5_3.index)) / (len(historical_df.index)), 4)
			nf5_4 = round((len(sf5_4.index)) / (len(historical_df.index)), 4)
			nf5_5 = round((len(sf5_5.index)) / (len(historical_df.index)), 4)
			nf5_6 = round((len(sf5_6.index)) / (len(historical_df.index)), 4)
			nf5_7 = round((len(sf5_7.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf5_5
			overestimating_rate = overestimating_rate + nf5_6 + nf5_7
			underestimating_rate = underestimating_rate + nf5_1 + nf5_2 + nf5_3 + nf5_4

			df.loc['<2 years (sim)', '25 years - 50 years (obs)'] = nf5_1
			df.loc['2 years - 5 years (sim)', '25 years - 50 years (obs)'] = nf5_2
			df.loc['5 years - 10 years (sim)', '25 years - 50 years (obs)'] = nf5_3
			df.loc['10 years - 25 years (sim)', '25 years - 50 years (obs)'] = nf5_4
			df.loc['25 years - 50 years (sim)', '25 years - 50 years (obs)'] = nf5_5
			df.loc['50 years - 100 years (sim)', '25 years - 50 years (obs)'] = nf5_6
			df.loc['>=100 years (sim)', '25 years - 50 years (obs)'] = nf5_7
			df.loc['Total (sim)', '25 years - 50 years (obs)'] = nf5_1 + nf5_2 + nf5_3 + nf5_4 + nf5_5 + nf5_6 + nf5_7

		df6 = historical_df.loc[(historical_df['Observed'] >= obs_50_threshold) & (historical_df['Observed'] < obs_100_threshold)]
		merged_df6 = pd.DataFrame.join(simulated_df, df6).dropna()
		merged_df6.drop('Observed', axis=1, inplace=True)

		if len(merged_df6.index) == 0:
			print('')
		else:
			sf6_1 = merged_df6.loc[(merged_df6['Simulated'] < sim_2_threshold)]
			sf6_2 = merged_df6.loc[(merged_df6['Simulated'] >= sim_2_threshold) & (merged_df6['Simulated'] < sim_5_threshold)]
			sf6_3 = merged_df6.loc[(merged_df6['Simulated'] >= sim_5_threshold) & (merged_df6['Simulated'] < sim_10_threshold)]
			sf6_4 = merged_df6.loc[(merged_df6['Simulated'] >= sim_10_threshold) & (merged_df6['Simulated'] < sim_25_threshold)]
			sf6_5 = merged_df6.loc[(merged_df6['Simulated'] >= sim_25_threshold) & (merged_df6['Simulated'] < sim_50_threshold)]
			sf6_6 = merged_df6.loc[(merged_df6['Simulated'] >= sim_50_threshold) & (merged_df6['Simulated'] < sim_100_threshold)]
			sf6_7 = merged_df6.loc[(merged_df6['Simulated'] >= sim_100_threshold)]

			nf6_1 = round((len(sf6_1.index)) / (len(historical_df.index)), 4)
			nf6_2 = round((len(sf6_2.index)) / (len(historical_df.index)), 4)
			nf6_3 = round((len(sf6_3.index)) / (len(historical_df.index)), 4)
			nf6_4 = round((len(sf6_4.index)) / (len(historical_df.index)), 4)
			nf6_5 = round((len(sf6_5.index)) / (len(historical_df.index)), 4)
			nf6_6 = round((len(sf6_6.index)) / (len(historical_df.index)), 4)
			nf6_7 = round((len(sf6_7.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf6_6
			overestimating_rate = overestimating_rate + nf6_7
			underestimating_rate = underestimating_rate + nf6_1 + nf6_2 + nf6_3 + nf6_4 + nf6_5

			df.loc['<2 years (sim)', '50 years - 100 years (obs)'] = nf6_1
			df.loc['2 years - 5 years (sim)', '50 years - 100 years (obs)'] = nf6_2
			df.loc['5 years - 10 years (sim)', '50 years - 100 years (obs)'] = nf6_3
			df.loc['10 years - 25 years (sim)', '50 years - 100 years (obs)'] = nf6_4
			df.loc['25 years - 50 years (sim)', '50 years - 100 years (obs)'] = nf6_5
			df.loc['50 years - 100 years (sim)', '50 years - 100 years (obs)'] = nf6_6
			df.loc['>=100 years (sim)', '50 years - 100 years (obs)'] = nf6_7
			df.loc['Total (sim)', '50 years - 100 years (obs)'] = nf6_1 + nf6_2 + nf6_3 + nf6_4 + nf6_5 + nf6_6 + nf6_7

		df7 = historical_df.loc[(historical_df['Observed'] >= obs_100_threshold)]
		merged_df7 = pd.DataFrame.join(simulated_df, df7).dropna()
		merged_df7.drop('Observed', axis=1, inplace=True)

		if len(merged_df7.index) == 0:
			print('')
		else:
			sf7_1 = merged_df7.loc[(merged_df7['Simulated'] < sim_2_threshold)]
			sf7_2 = merged_df7.loc[(merged_df7['Simulated'] >= sim_2_threshold) & (merged_df7['Simulated'] < sim_5_threshold)]
			sf7_3 = merged_df7.loc[(merged_df7['Simulated'] >= sim_5_threshold) & (merged_df7['Simulated'] < sim_10_threshold)]
			sf7_4 = merged_df7.loc[(merged_df7['Simulated'] >= sim_10_threshold) & (merged_df7['Simulated'] < sim_25_threshold)]
			sf7_5 = merged_df7.loc[(merged_df7['Simulated'] >= sim_25_threshold) & (merged_df7['Simulated'] < sim_50_threshold)]
			sf7_6 = merged_df7.loc[(merged_df7['Simulated'] >= sim_50_threshold) & (merged_df7['Simulated'] < sim_100_threshold)]
			sf7_7 = merged_df7.loc[(merged_df7['Simulated'] >= sim_100_threshold)]

			nf7_1 = round((len(sf7_1.index)) / (len(historical_df.index)), 4)
			nf7_2 = round((len(sf7_2.index)) / (len(historical_df.index)), 4)
			nf7_3 = round((len(sf7_3.index)) / (len(historical_df.index)), 4)
			nf7_4 = round((len(sf7_4.index)) / (len(historical_df.index)), 4)
			nf7_5 = round((len(sf7_5.index)) / (len(historical_df.index)), 4)
			nf7_6 = round((len(sf7_6.index)) / (len(historical_df.index)), 4)
			nf7_7 = round((len(sf7_7.index)) / (len(historical_df.index)), 4)

			success_rate = success_rate + nf7_7
			overestimating_rate = overestimating_rate
			underestimating_rate = underestimating_rate + nf7_1 + nf7_2 + nf7_3 + nf7_4 + nf7_5 + nf7_6

			df.loc['<2 years (sim)', '>=100 years (obs)'] = nf7_1
			df.loc['2 years - 5 years (sim)', '>=100 years (obs)'] = nf7_2
			df.loc['5 years - 10 years (sim)', '>=100 years (obs)'] = nf7_3
			df.loc['10 years - 25 years (sim)', '>=100 years (obs)'] = nf7_4
			df.loc['25 years - 50 years (sim)', '>=100 years (obs)'] = nf7_5
			df.loc['50 years - 100 years (sim)', '>=100 years (obs)'] = nf7_6
			df.loc['>=100 years (sim)', '>=100 years (obs)'] = nf7_7
			df.loc['Total (sim)', '>=100 years (obs)'] = nf7_1 + nf7_2 + nf7_3 + nf7_4 + nf7_5 + nf7_6 + nf7_7

		# Finishing filling matrix
		df.loc['<2 years (sim)', 'Total (obs)'] = df.loc['<2 years (sim)', '<2 years (obs)'] + df.loc['<2 years (sim)', '2 years - 5 years (obs)'] + df.loc['<2 years (sim)', '5 years - 10 years (obs)'] + df.loc['<2 years (sim)', '10 years - 25 years (obs)'] + df.loc['<2 years (sim)', '25 years - 50 years (obs)'] + df.loc['<2 years (sim)', '50 years - 100 years (obs)'] + df.loc['<2 years (sim)', '>=100 years (obs)']
		df.loc['2 years - 5 years (sim)', 'Total (obs)'] = df.loc['2 years - 5 years (sim)', '<2 years (obs)'] + df.loc['2 years - 5 years (sim)', '2 years - 5 years (obs)'] + df.loc['2 years - 5 years (sim)', '5 years - 10 years (obs)'] + df.loc['2 years - 5 years (sim)', '10 years - 25 years (obs)'] + df.loc['2 years - 5 years (sim)', '25 years - 50 years (obs)'] + df.loc['2 years - 5 years (sim)', '50 years - 100 years (obs)'] + df.loc['2 years - 5 years (sim)', '>=100 years (obs)']
		df.loc['5 years - 10 years (sim)', 'Total (obs)'] = df.loc['5 years - 10 years (sim)', '<2 years (obs)'] + df.loc['5 years - 10 years (sim)', '2 years - 5 years (obs)'] + df.loc['5 years - 10 years (sim)', '5 years - 10 years (obs)'] + df.loc['5 years - 10 years (sim)', '10 years - 25 years (obs)'] + df.loc['5 years - 10 years (sim)', '25 years - 50 years (obs)'] + df.loc['5 years - 10 years (sim)', '50 years - 100 years (obs)'] + df.loc['5 years - 10 years (sim)', '>=100 years (obs)']
		df.loc['10 years - 25 years (sim)', 'Total (obs)'] = df.loc['10 years - 25 years (sim)', '<2 years (obs)'] + df.loc['10 years - 25 years (sim)', '2 years - 5 years (obs)'] + df.loc['10 years - 25 years (sim)', '5 years - 10 years (obs)'] + df.loc['10 years - 25 years (sim)', '10 years - 25 years (obs)'] + df.loc['10 years - 25 years (sim)', '25 years - 50 years (obs)'] + df.loc['10 years - 25 years (sim)', '50 years - 100 years (obs)'] + df.loc['10 years - 25 years (sim)', '>=100 years (obs)']
		df.loc['25 years - 50 years (sim)', 'Total (obs)'] = df.loc['25 years - 50 years (sim)', '<2 years (obs)'] + df.loc['25 years - 50 years (sim)', '2 years - 5 years (obs)'] + df.loc['25 years - 50 years (sim)', '5 years - 10 years (obs)'] + df.loc['25 years - 50 years (sim)', '10 years - 25 years (obs)'] + df.loc['25 years - 50 years (sim)', '25 years - 50 years (obs)'] + df.loc['25 years - 50 years (sim)', '50 years - 100 years (obs)'] + df.loc['25 years - 50 years (sim)', '>=100 years (obs)']
		df.loc['50 years - 100 years (sim)', 'Total (obs)'] = df.loc['50 years - 100 years (sim)', '<2 years (obs)'] + df.loc['50 years - 100 years (sim)', '2 years - 5 years (obs)'] + df.loc['50 years - 100 years (sim)', '5 years - 10 years (obs)'] + df.loc['50 years - 100 years (sim)', '10 years - 25 years (obs)'] + df.loc['50 years - 100 years (sim)', '25 years - 50 years (obs)'] + df.loc['50 years - 100 years (sim)', '50 years - 100 years (obs)'] + df.loc['50 years - 100 years (sim)', '>=100 years (obs)']
		df.loc['>=100 years (sim)', 'Total (obs)'] = df.loc['>=100 years (sim)', '<2 years (obs)'] + df.loc['>=100 years (sim)', '2 years - 5 years (obs)'] + df.loc['>=100 years (sim)', '5 years - 10 years (obs)'] + df.loc['>=100 years (sim)', '10 years - 25 years (obs)'] + df.loc['>=100 years (sim)', '25 years - 50 years (obs)'] + df.loc['>=100 years (sim)', '50 years - 100 years (obs)'] + df.loc['>=100 years (sim)', '>=100 years (obs)']
		df.loc['Total (sim)', 'Total (obs)'] = df.loc['Total (sim)', '<2 years (obs)'] + df.loc['Total (sim)', '2 years - 5 years (obs)'] + df.loc['Total (sim)', '5 years - 10 years (obs)'] + df.loc['Total (sim)', '10 years - 25 years (obs)'] + df.loc['Total (sim)', '25 years - 50 years (obs)'] + df.loc['Total (sim)', '50 years - 100 years (obs)'] + df.loc['Total (sim)', '>=100 years (obs)']

		success_rate_ini = success_rate
		overestimating_rate_ini = overestimating_rate
		underestimating_rate_ini = underestimating_rate

		#print(df)
		#print(' ')
		#print('El porcentaje deéxito para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((success_rate * 100), 2)) + '%')
		#print('El porcentaje desobre-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((overestimating_rate * 100), 2)) + '%')
		#print('El porcentaje desub-estimación para la inicialización del pronóstico en la estación {0} es: '.format(name) + str(round((underestimating_rate * 100), 2)) + '%')
		#print(' ')
		#print(' ')

	contingency_matrix.append([id, comid, name, event_return_period, df.loc['Total (sim)', 'Total (obs)'], success_rate_ini, overestimating_rate_ini, underestimating_rate_ini, overestimating_rate_ini+underestimating_rate_ini])

contingency_matrix_df = pd.DataFrame(contingency_matrix, columns=['Code', 'COMID', 'Station', 'Return Period', 'Total Percentage', 'Success (Hits) Rate', 'Overestimating Rate', 'Underestimating Rate', 'Misses Rate'])
print(contingency_matrix_df)

#Original Simulated Data
#contingency_matrix_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Bolivia/data/historical/validationResults_Original/Tables/Contingency_Matrix_Before_Bias_Correction.csv')

#Bias Corrected Simulated Data
contingency_matrix_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Bolivia/data/historical/validationResults_Corrected/Tables/Contingency_Matrix_After_Bias_Correction.csv')
