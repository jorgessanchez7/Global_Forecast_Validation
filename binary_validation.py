import pandas as pd

df = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Stations_Selected_Colombia_RT.csv')

IDs = df['Codigo'].tolist()
COMIDs = df['COMID'].tolist()
Names = df['Nombre'].tolist()
Rivers = df['Corriente'].tolist()

'''Get Historical Observed Water Levels'''

observed_wl_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Water_Level'

waterLevelData = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_IDEAM/NIVEL.tr5.csv', index_col=1)
waterLevelData.index = pd.to_datetime(waterLevelData.index)
fechas = waterLevelData.index.tolist()
estaciones = waterLevelData['ESTACION'].tolist()
valores = waterLevelData['VALOR'].tolist()

for id in IDs:
	waterLevel = []
	dates = []

	for i in range (0, len(estaciones)):
		if (id == estaciones[i]):
			waterLevel.append(valores[i])
			dates.append(fechas[i])

	pairs = [list(a) for a in zip(dates, waterLevel)]
	pd.DataFrame(pairs, columns= ['Datetime', 'oberved water level (cm)']).to_csv(observed_wl_dir + "/{}_historic_observed_water_level.csv".format(id), encoding='utf-8', header=True, index=0)
	print("{}_historic_observed_water_level.csv".format(id))


#Reading historic simulated and historic observed data

historicSimulatedFiles = []
historicObservedFiles = []
historicWaterLevelFiles = []

for id, comid in zip(IDs, COMIDs):
	historicObservedFiles.append('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Observed/' + str(id) + '_historic_observed.csv')
	historicSimulatedFiles.append('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Simulated/' + str(comid) + '_historic_simulatied.csv')
	historicSimulatedFiles.append('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Historical_Water_Level/' + str(id) + '_historic_observed_water_level.csv')

for id, comid, name, rio, obsFile, simFile, wlFile in zip(IDs, COMIDs, Names, Rivers, historicObservedFiles, historicSimulatedFiles, historicWaterLevelFiles):
	print(id, comid, name, rio)

'''Get Real Time Observed Water Levels'''

observed_WL_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Real_Time_Water_Level'
daily_wl_dir = '/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Daily_Real_Time_Water_Level'

waterLevelData = pd.read_csv('/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Real_Time_IDEAM/NIVEL-DHIME.csv', index_col=9)
waterLevelData.index = pd.to_datetime(waterLevelData.index)
fechas = waterLevelData.index.tolist()
estaciones = waterLevelData['CodigoEstacion'].tolist()
valores = waterLevelData['Valor'].tolist()

# for id in IDs:
	# waterLevel = []
	# dates = []
	#
	# for i in range (0, len(estaciones)):
	# 	if (id == estaciones[i]):
	# 		waterLevel.append(valores[i])
	# 		dates.append(fechas[i])
	#
	# pairs = [list(a) for a in zip(dates, waterLevel)]
	# pd.DataFrame(pairs, columns= ['Datetime', 'oberved water level (cm)']).to_csv(observed_WL_dir + "/{}_real_time_observed_water_level.csv".format(id), encoding='utf-8', header=True, index=0)
	# print("{}_real_time_observed_water_level.csv".format(id))
	#
	# data = pd.read_csv("/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Real_Time_Water_Level/{0}_real_time_observed_water_level.csv".format(id), index_col=0)
	#
	# data.index = pd.to_datetime(data.index)
	#
	# daily_df = data.groupby(data.index.strftime("%Y/%m/%d")).mean()
	# daily_df.index = pd.to_datetime(daily_df.index)
	#
	# daily_df.to_csv("/Users/student/Dropbox/PhD/2019 Summer/Dissertation_v7/Colombia/Data/Daily_Real_Time_Water_Level/{0}_real_time_observed_water_level.csv".format(id), index_label="Datetime")
	#
	# print(daily_df)





#Defining the return periods for the historical Simulation