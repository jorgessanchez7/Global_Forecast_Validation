import pandas as pd

metrics_bbc = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Brazil/data/historical/validationResults_Original/Tables/Metrics_Before_Bias_Correction_Brazil.csv')

IDs = metrics_bbc['ID'].tolist()
COMIDs = metrics_bbc['COMID'].tolist()
Names = metrics_bbc['Station'].tolist()
Latitude = metrics_bbc['Latitude'].tolist()
Longitude = metrics_bbc['Longitude'].tolist()
Bias_bbc = metrics_bbc['Bias'].tolist()
Variability_bbc = metrics_bbc['Variability'].tolist()
R_Pearson_bbc = metrics_bbc['R (Pearson)'].tolist()
KGE_2012_bbc = metrics_bbc['KGE (2012)'].tolist()

metrics_bbc = pd.read_csv('/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Brazil/data/historical/validationResults_Corrected/Tables/Metrics_After_Bias_Correction_Brazil.csv')

Bias_abc = metrics_bbc['Bias'].tolist()
Variability_abc = metrics_bbc['Variability'].tolist()
R_Pearson_abc = metrics_bbc['R (Pearson)'].tolist()
KGE_2012_abc = metrics_bbc['KGE (2012)'].tolist()

bias = []
variability = []
r_pearson = []
kge_2012 = []

for id, comid, name, lat, lon, bias1, bias2, variability1, variability2, corr1, corr2, kge1, kge2 in zip(IDs, COMIDs, Names, Latitude, Longitude, Bias_bbc, Bias_abc, Variability_bbc, Variability_abc, R_Pearson_bbc, R_Pearson_abc, KGE_2012_bbc, KGE_2012_abc):

	if abs(bias1-1) < abs(bias2-1):
		delta_bias = abs(bias2-1) - abs(bias1-1)
		bias.append([id, comid, name, lat, lon, bias1, bias2, delta_bias])

	if abs(variability1-1) < abs(variability2-1):
		delta_variability = abs(variability2 - 1) - abs(variability1 - 1)
		variability.append([id, comid, name, lat, lon, variability1, variability2, delta_variability])

	if corr1 > corr2:
		r_pearson.append([id, comid, name, lat, lon, corr1, corr2])

	if kge1 > kge2:
		kge_2012.append([id, comid, name, lat, lon, kge1, kge2])

bias_df = pd.DataFrame(bias, columns=['ID', 'COMID', 'Station', 'Latitude', 'Longitude', 'Bias Original', 'Bias Corrected', 'Delta Bias'])

variability_df = pd.DataFrame(variability, columns=['ID', 'COMID', 'Station', 'Latitude', 'Longitude', 'Variability Original', 'Variability Corrected', 'Delta Variability'])

r_pearson_df = pd.DataFrame(r_pearson, columns=['ID', 'COMID', 'Station', 'Latitude', 'Longitude', 'R_pearson Original', 'R_pearson Corrected'])

kge_2012_df = pd.DataFrame(kge_2012, columns=['ID', 'COMID', 'Station', 'Latitude', 'Longitude', 'KGE_2012 Original', 'KGE_2012 Corrected'])


print('bias')
print(bias_df)

print('variability')
print(variability_df)

print('r_pearson')
print(r_pearson_df)

print('kge_2012')
print(kge_2012_df)