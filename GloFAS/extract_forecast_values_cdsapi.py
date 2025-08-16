import xarray as xr
import pandas as pd

lati = 5.475
longi = -74.675

anio = '2025'
mes = '06'
dia = '10'

fecha = '{0}{1}{2}'.format(anio, mes, dia)

nc_file_ini = 'D:\\GloFAS_Forecast\\{0}\\data_0.nc'.format(fecha)
dataset_ini = xr.open_dataset(nc_file_ini, autoclose=True)
qout_datasets_ini = dataset_ini.sel(longitude= longi, method='nearest').sel(latitude= lati, method='nearest').dis24
time_dataset_ini = qout_datasets_ini.valid_time

ini_df = pd.DataFrame(qout_datasets_ini, index=time_dataset_ini, columns=['Streamflow (m3/s)'])

print(ini_df)

files = [
    "D:\\GloFAS_Forecast\\{0}\\data_0.nc".format(fecha),
    "D:\\GloFAS_Forecast\\{0}\\data_1.nc".format (fecha)
]

forecast_df = pd.DataFrame()

for file in files:
    ds = xr.open_dataset(file)

    forecast_datetimes = pd.to_datetime(ds['valid_time'].values)

    # Seleccionar descarga más cercana
    discharge = ds['dis24'].sel(latitude=lati, longitude=longi, method='nearest')

    # Si forecast_df aún no tiene índice, establecerlo ahora
    if forecast_df.empty:
        forecast_df = pd.DataFrame(index=forecast_datetimes)

    if 'number' in discharge.dims:
        # Múltiples ensambles (data_1.nc)
        for ens in discharge['number'].values:
            col_name = f"ensemble_{int(ens):02d}"
            forecast_df[col_name] = discharge.sel(number=ens).values
    else:
        # Solo 1 ensamble (data_0.nc)
        forecast_df['ensemble_00'] = discharge.values

# Reemplazar negativos por cero
forecast_df[forecast_df < 0] = 0

# Mostrar resultado
print(forecast_df)
