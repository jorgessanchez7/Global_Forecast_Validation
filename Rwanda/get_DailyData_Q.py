import numpy as np
import pandas as pd

df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Rwanda/Stations_Rwanda.csv')
df = df.loc[df['Q'] =='YES']

IDs = df['samplingFeatureCode'].tolist()
names = df['name'].tolist()
rivers = df['River'].tolist()

for id, river, name in zip(IDs, rivers, names):

	print(id, name, river)

	data = pd.read_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Rwanda/intermediate/{0}_Q.csv".format(id), index_col=0)

	data.index = pd.to_datetime(data.index)

	daily_df = data.groupby(data.index.strftime("%Y/%m/%d")).mean()
	daily_df.index = pd.to_datetime(daily_df.index)
	print(daily_df)

	datesObservedDischarge = pd.date_range(daily_df.index[0], daily_df.index[len(daily_df.index) - 1], freq='D')
	df2 = pd.DataFrame(np.nan, index=datesObservedDischarge, columns=['Streamflow (m3/s)'])
	df2.index.name = 'Datetime'

	df3 = df2.fillna(daily_df)

	df3.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Rwanda/Data/Observed_Streamflow/{0}.csv".format(id), index_label="Datetime")

	df3.fillna(-9999, inplace=True)
	
	df3.to_csv("/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/Global_Hydroserver/Observed_Data/Africa/Rwanda/{0}_Q.csv".format(id), index_label="Datetime")