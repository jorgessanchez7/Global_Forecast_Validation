import xarray as xr
import pandas as pd

lati = 5.475
longi = -74.675

anio = '2025'
mes = '08'
dia = '01'

fecha = '{0}{1}{2}'.format(anio, mes, dia)

forecast_df = pd.DataFrame()

ensembles = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17',
             '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35',
             '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50']

nc_file_ini = 'D:\\GloFAS_Forecast\\{0}\\initial_conditions (2).nc'.format(fecha)
dataset_ini = xr.open_dataset(nc_file_ini, autoclose=True)
qout_datasets_ini = dataset_ini.sel(longitude= longi, method='nearest').sel(latitude= lati, method='nearest').dis24
time_dataset_ini = qout_datasets_ini.valid_time

ini_df = pd.DataFrame(qout_datasets_ini, index=time_dataset_ini, columns=['Streamflow (m3/s)'])

print(ini_df)

for ensemble in ensembles:

  nc_file = 'D:\\GloFAS_Forecast\\{0}\\dis_{1}_{0}00.nc'.format(fecha, ensemble)
  dataset = xr.open_dataset(nc_file, autoclose=True)

  qout_datasets = dataset.sel(lon= longi, method='nearest').sel(lat= lati, method='nearest').dis
  time_dataset = qout_datasets.time

  ens = int(ensemble) + 1

  if ens < 10:
    ens = '0{0}'.format(str(ens))
  else:
    ens = str(ens)

  file_df = pd.DataFrame(qout_datasets, index=time_dataset, columns=['ensemble_{0}'.format(ens)])
  file_df.index.name = 'datetime'
  file_df.sort_index(inplace=True)
  file_df[file_df < 0] = 0
  file_df.index = pd.to_datetime(file_df.index)
  file_df.index = file_df.index.to_series().dt.strftime("%Y-%m-%d")
  file_df.index = pd.to_datetime(file_df.index)

  forecast_df = pd.concat([forecast_df, file_df], axis=1)

print(forecast_df)