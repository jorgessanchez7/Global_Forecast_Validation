import os
import pandas as pd
import datetime as dt

comids = [9020413, 9021824, 9011627, 9011802, 9012220, 9009660, 9013878, 9015841, 9015333, 9015566, 9005832]

print(comids)

# Function to extract forecast values for a given reach_id
from glob import glob
import xarray

def get_ecmwf_ensemble(path_data, output_path, forecast_date, river_id):
    """
    Returns the statistics for the 52 member forecast
    path_data: folder where the forecasts files are stored
    river_id: river stream which we want to get the forecast values
    forecast_date: date to be extractd in format YYYYMMDD
    """

    path_to_files = path_data + '/' + forecast_date + '.00'

    forecast_nc_list = sorted(glob(os.path.join(path_to_files, "*.nc")), reverse=True)

    # print(forecast_nc_list)

    # combine 52 ensembles
    qout_datasets = []
    ensemble_index_list = []

    for forecast_nc in forecast_nc_list:
        ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
        qout_datasets.append(xarray.open_dataset(forecast_nc, autoclose=True).sel(rivid=river_id).Qout)

    merged_ds = xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))

    return_dict = {}
    for i in range(1, 52):
        return_dict['ensemble_{0}'.format(i)] = merged_ds.sel(ensemble=i).dropna('time')

    forecast_df = pd.DataFrame(return_dict)
    time_array = return_dict['ensemble_1']['time']
    forecast_df['datetime'] = time_array
    forecast_df.set_index('datetime', inplace=True)
    forecast_df.rename(columns={"ensemble_1": "ensemble_01_m^3/s", "ensemble_2": "ensemble_02_m^3/s",
                                "ensemble_3": "ensemble_03_m^3/s",
                                "ensemble_4": "ensemble_04_m^3/s", "ensemble_5": "ensemble_05_m^3/s",
                                "ensemble_6": "ensemble_06_m^3/s",
                                "ensemble_7": "ensemble_07_m^3/s", "ensemble_8": "ensemble_08_m^3/s",
                                "ensemble_9": "ensemble_09_m^3/s",
                                "ensemble_10": "ensemble_10_m^3/s", "ensemble_11": "ensemble_11_m^3/s",
                                "ensemble_12": "ensemble_12_m^3/s",
                                "ensemble_13": "ensemble_13_m^3/s", "ensemble_14": "ensemble_14_m^3/s",
                                "ensemble_15": "ensemble_15_m^3/s",
                                "ensemble_16": "ensemble_16_m^3/s", "ensemble_17": "ensemble_17_m^3/s",
                                "ensemble_18": "ensemble_18_m^3/s",
                                "ensemble_19": "ensemble_19_m^3/s", "ensemble_20": "ensemble_20_m^3/s",
                                "ensemble_21": "ensemble_21_m^3/s",
                                "ensemble_22": "ensemble_22_m^3/s", "ensemble_23": "ensemble_23_m^3/s",
                                "ensemble_24": "ensemble_24_m^3/s",
                                "ensemble_25": "ensemble_25_m^3/s", "ensemble_26": "ensemble_26_m^3/s",
                                "ensemble_27": "ensemble_27_m^3/s",
                                "ensemble_28": "ensemble_28_m^3/s", "ensemble_29": "ensemble_29_m^3/s",
                                "ensemble_30": "ensemble_30_m^3/s",
                                "ensemble_31": "ensemble_31_m^3/s", "ensemble_32": "ensemble_32_m^3/s",
                                "ensemble_33": "ensemble_33_m^3/s",
                                "ensemble_34": "ensemble_34_m^3/s", "ensemble_35": "ensemble_35_m^3/s",
                                "ensemble_36": "ensemble_36_m^3/s",
                                "ensemble_37": "ensemble_37_m^3/s", "ensemble_38": "ensemble_38_m^3/s",
                                "ensemble_39": "ensemble_39_m^3/s",
                                "ensemble_40": "ensemble_40_m^3/s", "ensemble_41": "ensemble_41_m^3/s",
                                "ensemble_42": "ensemble_42_m^3/s",
                                "ensemble_43": "ensemble_43_m^3/s", "ensemble_44": "ensemble_44_m^3/s",
                                "ensemble_45": "ensemble_45_m^3/s",
                                "ensemble_46": "ensemble_46_m^3/s", "ensemble_47": "ensemble_47_m^3/s",
                                "ensemble_48": "ensemble_48_m^3/s",
                                "ensemble_49": "ensemble_49_m^3/s", "ensemble_50": "ensemble_50_m^3/s",
                                "ensemble_51": "ensemble_51_m^3/s"
                                }, inplace=True)

    return_dict = {}
    return_dict['ensemble_52'.format(i)] = merged_ds.sel(ensemble=i).dropna('time')

    high_res_df = pd.DataFrame(return_dict)
    time_array_2 = return_dict['ensemble_52']['time']
    high_res_df['datetime'] = time_array_2
    high_res_df.set_index('datetime', inplace=True)
    high_res_df.rename(columns={"ensemble_52": "ensemble_52_m^3/s"}, inplace=True)

    if not os.path.isdir(output_path + '/' + str(river_id) + '/row_data'):
        os.makedirs(output_path + '/' + str(river_id) + '/row_data', exist_ok=True)

    forecast_df.to_csv(
        output_path + '/' + str(river_id) + '/row_data/' + str(forecast_date)[0:4] + '-' + str(forecast_date)[
                                                                                           4:6] + '-' + str(
            forecast_date)[6:8] + '.csv')
    high_res_df.to_csv(
        output_path + '/' + str(river_id) + '/row_data/' + str(forecast_date)[0:4] + '-' + str(forecast_date)[
                                                                                           4:6] + '-' + str(
            forecast_date)[6:8] + '_HR.csv')


fecha_ini = dt.datetime(2014, 1, 1)
fecha_fin = dt.datetime(2019, 12, 31)
fechas = pd.date_range(fecha_ini, fecha_fin, freq='D')

path_data = '/Volumes/GoogleDrive/My Drive/ECMWF/ECMWF_GEOGloWS_Streamflow/rapid-io/output/south_america-geoglows/'
output_path = '/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/Colombia'

for fecha in fechas:
    anio = str(fecha.year)
    mes = fecha.month
    if mes < 10:
        mes = '0' + str(mes)
    else:
        mes = str(mes)
    dia = fecha.day
    if dia < 10:
        dia = '0' + str(dia)
    else:
        dia = str(dia)

    forecast_date = '{0}{1}{2}'.format(anio, mes, dia)

    print(forecast_date)

    for comid in comids:
        get_ecmwf_ensemble(path_data, output_path, forecast_date, int(comid))