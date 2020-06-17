import pandas as pd
import geoglows

#regions = ['central_america-geoglows', 'south_america-geoglows', 'north_america-geoglows', 'australia-geoglows',
#           'africa-geoglows','central_asia-geoglows', 'east_asia-geoglows', 'europe-geoglows',
#           'islands-geoglows', 'japan-geoglows', 'middle_east-geoglows', 'south_asia-geoglows',
#           'west_asia-geoglows']

regions = ['central_america-geoglows', 'south_america-geoglows', 'north_america-geoglows', 'australia-geoglows',
           'africa-geoglows']

for region in regions:

	df = pd.read_csv('/Volumes/BYU_HD/Streamflow_Prediction_Tool/Shapes/{0}-drainageline_2.csv'.format(region))

	COMIDs = df['COMID'].tolist()

	for comid in COMIDs:

		print (region, '-', comid)

		'''Get Era_5 Data'''
		df1 = geoglows.streamflow.historic_simulation(comid, forcing='era_5', return_format='csv')
		df1[df1 < 0] = 0
		df1.index = df1.index.to_series().dt.strftime("%Y-%m-%d")
		df1.index = pd.to_datetime(df1.index)

		pairs = [list(a) for a in zip(df1.index, df1.iloc[:,0].values)]
		era5_df = pd.DataFrame(pairs, columns=['Datetime', 'era5 Streamflow (m3/s)'])
		era5_df.set_index('Datetime', inplace=True)
		era5_df.index = pd.to_datetime(era5_df.index)

		'''Get Era_Interim Data'''
		df2 = geoglows.streamflow.historic_simulation(comid, forcing='era_interim', return_format='csv')
		df2[df2 < 0] = 0
		df2.index = df2.index.to_series().dt.strftime("%Y-%m-%d")
		df2.index = pd.to_datetime(df2.index)

		pairs = [list(a) for a in zip(df2.index, df2.iloc[:, 0].values)]
		eraI_df = pd.DataFrame(pairs, columns=['Datetime', 'erai Streamflow (m3/s)'])
		eraI_df.set_index('Datetime', inplace=True)
		eraI_df.index = pd.to_datetime(eraI_df.index)

		'''Writing Files'''
		era5_df.to_csv('/Volumes/BYU_HD/Streamflow_Prediction_Tool/Time_Series/ERA_5/{0}/{1}.csv'.format(region, comid))
		eraI_df.to_csv('/Volumes/BYU_HD/Streamflow_Prediction_Tool/Time_Series/ERA_Interim/{0}/{1}.csv'.format(region, comid))

	print ('done for the region: {0}'.format(region))

print ('downloaded for all the regions')