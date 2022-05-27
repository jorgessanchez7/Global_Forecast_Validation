from climata.usgs import DailyValueIO
import pandas as pd
import numpy as np
import datetime as dt

stations_pd = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/Total_Stations_USA_Q_v1.csv')

IDs = stations_pd['STAID'].tolist()
COMIDs = stations_pd['new_COMID'].tolist()
Names = stations_pd['STANAME'].tolist()


for id, name, comid in zip(IDs, Names, COMIDs):

	if id < 10000000:
		station_id = '0' + str(id)
	else:
		station_id = str(id)
	param_id = "00060" #streamflow
	#param_id = "00065"  #water level

	print(station_id, ' - ', name, ' - ', comid)

	datelist = pd.date_range(dt.datetime(1900, 1, 1), dt.datetime.today(), freq='D').tolist()
	data = DailyValueIO(
		start_date=datelist[0],
		end_date=datelist[-1],
		station=station_id,
		parameter=param_id,
	)

	for series in data:
		flow = [r[1] for r in series.data]
		dates = [r[0] for r in series.data]

	flow_cms = []

	for sfv in flow:
		if sfv == -999999:
			flow_cms.append(np.nan)
		else:
			flow_cms.append(sfv * 0.028316847)

	df = pd.DataFrame(flow_cms, index=dates, columns=['Streamflow (m3/s)'])
	df.dropna(inplace=True)

	datesObservedDischarge = pd.date_range(df.index[0], df.index[len(df.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Streamflow (m3/s)'])
	df2.index.name = 'Datetime'

	observed_df = df2.fillna(df)
	observed_df[observed_df < 0] = 0
	observed_df.index = pd.to_datetime(observed_df.index)
	observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
	observed_df.index = pd.to_datetime(observed_df.index)

	observed_df.to_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/North_America/USA/data/historical/Observed_Data/{}.csv'.format(station_id))