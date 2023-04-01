import os
import pytz
import pandas as pd
import datetime as dt
from functools import reduce


import warnings
warnings.filterwarnings('ignore')

country = 'north_america'
region = 'north_america-geoglows'

comids = [13022647, 13023633, 13024715, 13031836, 13034791, 13036172, 13042754, 13047286, 13050580, 13050934, 13056466, 13059018, 13060624, 13061217, 13061403,
            13061935, 13062681, 13063686, 13064190, 13065316, 13065869, 13066312, 13066645, 13066938, 13067349, 13068517, 13068688, 13069419, 13069982, 13070658,
            13070728, 13071154, 13071884, 13072677, 13075094, 13075863, 13076155, 13076260, 13076497, 13078036, 13078407, 13079441, 13079513, 13079600, 13079923,
            13080006, 13080308, 13080336, 13080360, 13080509, 13080578, 13080623, 13080707, 13080729, 13081119, 13081136, 13081225, 13081255, 13081389, 13081932,
            13082014, 13082129, 13082244, 13082289, 13082311, 13082371, 13082414, 13082453, 13082493]
print(comids)

#Get the available dates
files = os.listdir('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/row_data'.format(country, str(comids[0])))
files2 = [val for val in files if not val.endswith("_HR.csv")]

dates = []

for file in files2:
  file = file[0:-4]
  dates.append(dt.datetime(int(file[0:4]), int(file[5:7]), int(file[8:10])))

dates = sorted(dates)
print(dates)

# Defining variables
day_forecast = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15']
day_forecast_HR = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
ensembles = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
             '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36',
             '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51']

for comid in comids:

    print(comid)

    initialization_dates = []
    initialization_values = []

    for days in day_forecast_HR:
        globals()['{}_Day_Forecasts_High_Res_dates'.format(days)] = []
        globals()['{}_Day_Forecasts_High_Res_values'.format(days)] = []

    for days in day_forecast:
        globals()['{}_Day_Forecasts_dates'.format(days)] = []

        for ensemble in ensembles:
            globals()['{0}_Day_Forecasts_ens_{1}_values'.format(days, ensemble)] = []

    for fecha in dates:

        print(fecha)

        YYYY = str(fecha.year)

        if fecha.month < 10:
            MM = '0{0}'.format(str(fecha.month))
        else:
            MM = str(fecha.month)

        if fecha.day < 10:
            DD = '0{0}'.format(str(fecha.day))
        else:
            DD = str(fecha.day)

        df_HR = pd.read_csv(
            '/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/row_data/{2}-{3}-{4}_HR.csv'.format(
                country, comid, YYYY, MM, DD), index_col=0)
        df_HR[df_HR < 0] = 0
        df_HR.index = pd.to_datetime(df_HR.index)
        df_HR.index = df_HR.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        df_HR.index = pd.to_datetime(df_HR.index, utc='UTC')

        initialization_dates.append(df_HR.index[0])
        initialization_values.append(df_HR.iloc[:, 0].values[0])

        for days in day_forecast_HR:
            ini_date = dt.datetime(fecha.year, fecha.month, fecha.day, tzinfo=pytz.UTC) + dt.timedelta(
                days=int(days) - 1)
            end_date = dt.datetime(fecha.year, fecha.month, fecha.day, tzinfo=pytz.UTC) + dt.timedelta(days=int(days))

            df1 = df_HR.loc[(df_HR.index > ini_date) & (df_HR.index <= end_date)]

            globals()['{}_Day_Forecasts_High_Res_dates'.format(days)].append(df1.index.tolist())
            globals()['{}_Day_Forecasts_High_Res_values'.format(days)].append(df1.iloc[:, 0].values.tolist())

        df = pd.read_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/row_data/{2}-{3}-{4}.csv'.format(country, comid, YYYY, MM, DD), index_col=0)
        df[df < 0] = 0
        df.index = pd.to_datetime(df.index)
        df.index = df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        df.index = pd.to_datetime(df.index)
        df.index = pd.to_datetime(df.index, utc='UTC')

        for days in day_forecast:

            ini_date = dt.datetime(fecha.year, fecha.month, fecha.day, tzinfo=pytz.UTC) + dt.timedelta(days=int(days) - 1)
            end_date = dt.datetime(fecha.year, fecha.month, fecha.day, tzinfo=pytz.UTC) + dt.timedelta(days=int(days))

            df2 = df.loc[(df.index > ini_date) & (df.index <= end_date)]

            globals()['{}_Day_Forecasts_dates'.format(days)].append(df2.index.tolist())

            for ensemble in ensembles:
                df3 = df2[['ensemble_{}_m^3/s'.format(ensemble)]].copy()

                globals()['{0}_Day_Forecasts_ens_{1}_values'.format(days, ensemble)].append(df3.iloc[:, 0].values.tolist())

    pairs = [list(a) for a in zip(initialization_dates, initialization_values)]
    Initialization = pd.DataFrame(pairs, columns=['Datetime', 'Initialization (m^3/s)'])
    Initialization.set_index('Datetime', inplace=True)
    # Adjusting the time zone of the forecast (originally in UTC) to the local time
    Initialization.index = Initialization.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    if not os.path.isdir("/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized".format(country, comid)):
        os.makedirs("/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized".format(country, comid))
    Initialization.to_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized/Initialization.csv'.format(country, comid))

    for days in day_forecast:

        globals()['{}_Day_Forecasts_dates'.format(days)] = reduce(lambda x, y: x + y, globals()['{}_Day_Forecasts_dates'.format(days)])

        for ensemble in ensembles:
            globals()['{0}_Day_Forecasts_ens_{1}_values'.format(days, ensemble)] = reduce(lambda x, y: x + y, globals()['{0}_Day_Forecasts_ens_{1}_values'.format(days, ensemble)])

        pairs = [list(a) for a in zip(globals()['{}_Day_Forecasts_dates'.format(days)], globals()['{0}_Day_Forecasts_ens_01_values'.format(days)], globals()['{0}_Day_Forecasts_ens_02_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_03_values'.format(days)], globals()['{0}_Day_Forecasts_ens_04_values'.format(days)], globals()['{0}_Day_Forecasts_ens_05_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_06_values'.format(days)], globals()['{0}_Day_Forecasts_ens_07_values'.format(days)], globals()['{0}_Day_Forecasts_ens_08_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_09_values'.format(days)], globals()['{0}_Day_Forecasts_ens_10_values'.format(days)], globals()['{0}_Day_Forecasts_ens_11_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_12_values'.format(days)], globals()['{0}_Day_Forecasts_ens_13_values'.format(days)], globals()['{0}_Day_Forecasts_ens_14_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_15_values'.format(days)], globals()['{0}_Day_Forecasts_ens_16_values'.format(days)], globals()['{0}_Day_Forecasts_ens_17_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_18_values'.format(days)], globals()['{0}_Day_Forecasts_ens_19_values'.format(days)], globals()['{0}_Day_Forecasts_ens_20_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_21_values'.format(days)], globals()['{0}_Day_Forecasts_ens_22_values'.format(days)], globals()['{0}_Day_Forecasts_ens_23_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_24_values'.format(days)], globals()['{0}_Day_Forecasts_ens_25_values'.format(days)], globals()['{0}_Day_Forecasts_ens_26_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_27_values'.format(days)], globals()['{0}_Day_Forecasts_ens_28_values'.format(days)], globals()['{0}_Day_Forecasts_ens_29_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_30_values'.format(days)], globals()['{0}_Day_Forecasts_ens_31_values'.format(days)], globals()['{0}_Day_Forecasts_ens_32_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_33_values'.format(days)], globals()['{0}_Day_Forecasts_ens_34_values'.format(days)], globals()['{0}_Day_Forecasts_ens_35_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_36_values'.format(days)], globals()['{0}_Day_Forecasts_ens_37_values'.format(days)], globals()['{0}_Day_Forecasts_ens_38_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_39_values'.format(days)], globals()['{0}_Day_Forecasts_ens_40_values'.format(days)], globals()['{0}_Day_Forecasts_ens_41_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_42_values'.format(days)], globals()['{0}_Day_Forecasts_ens_43_values'.format(days)], globals()['{0}_Day_Forecasts_ens_44_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_45_values'.format(days)], globals()['{0}_Day_Forecasts_ens_46_values'.format(days)], globals()['{0}_Day_Forecasts_ens_47_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_48_values'.format(days)], globals()['{0}_Day_Forecasts_ens_49_values'.format(days)], globals()['{0}_Day_Forecasts_ens_50_values'.format(days)],
                                      globals()['{0}_Day_Forecasts_ens_51_values'.format(days)])]

        globals()['{}_Day_Forecasts'.format(days)] = pd.DataFrame(pairs, columns=['Datetime', 'Ensemble 01 (m^3/s)', 'Ensemble 02 (m^3/s)', 'Ensemble 03 (m^3/s)', 'Ensemble 04 (m^3/s)', 'Ensemble 05 (m^3/s)', 'Ensemble 06 (m^3/s)',
                                                                                  'Ensemble 07 (m^3/s)', 'Ensemble 08 (m^3/s)', 'Ensemble 09 (m^3/s)', 'Ensemble 10 (m^3/s)', 'Ensemble 11 (m^3/s)', 'Ensemble 12 (m^3/s)', 'Ensemble 13 (m^3/s)',
                                                                                  'Ensemble 14 (m^3/s)', 'Ensemble 15 (m^3/s)', 'Ensemble 16 (m^3/s)', 'Ensemble 17 (m^3/s)', 'Ensemble 18 (m^3/s)', 'Ensemble 19 (m^3/s)', 'Ensemble 20 (m^3/s)',
                                                                                  'Ensemble 21 (m^3/s)', 'Ensemble 22 (m^3/s)', 'Ensemble 23 (m^3/s)', 'Ensemble 24 (m^3/s)', 'Ensemble 25 (m^3/s)', 'Ensemble 26 (m^3/s)', 'Ensemble 27 (m^3/s)',
                                                                                  'Ensemble 28 (m^3/s)', 'Ensemble 29 (m^3/s)', 'Ensemble 30 (m^3/s)', 'Ensemble 31 (m^3/s)', 'Ensemble 32 (m^3/s)', 'Ensemble 33 (m^3/s)', 'Ensemble 34 (m^3/s)',
                                                                                  'Ensemble 35 (m^3/s)', 'Ensemble 36 (m^3/s)', 'Ensemble 37 (m^3/s)', 'Ensemble 38 (m^3/s)', 'Ensemble 39 (m^3/s)', 'Ensemble 40 (m^3/s)', 'Ensemble 41 (m^3/s)',
                                                                                  'Ensemble 42 (m^3/s)', 'Ensemble 43 (m^3/s)', 'Ensemble 44 (m^3/s)', 'Ensemble 45 (m^3/s)', 'Ensemble 46 (m^3/s)', 'Ensemble 47 (m^3/s)', 'Ensemble 48 (m^3/s)',
                                                                                  'Ensemble 49 (m^3/s)', 'Ensemble 50 (m^3/s)', 'Ensemble 51 (m^3/s)'])

        globals()['{}_Day_Forecasts'.format(days)].set_index('Datetime', inplace=True)
        # Adjusting the time zone of the forecast (originally in UTC) to the local time
        globals()['{}_Day_Forecasts'.format(days)].index = globals()['{}_Day_Forecasts'.format(days)].index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        globals()['{}_Day_Forecasts'.format(days)].to_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized/{2}_Day_Forecasts.csv'.format(country, comid, days))

    for days in day_forecast_HR:
        globals()['{}_Day_Forecasts_High_Res_dates'.format(days)] = reduce(lambda x, y: x + y, globals()['{}_Day_Forecasts_High_Res_dates'.format(days)])
        globals()['{}_Day_Forecasts_High_Res_values'.format(days)] = reduce(lambda x, y: x + y, globals()['{}_Day_Forecasts_High_Res_values'.format(days)])
        pairs = [list(a) for a in zip(globals()['{}_Day_Forecasts_High_Res_dates'.format(days)], globals()['{}_Day_Forecasts_High_Res_values'.format(days)])]
        globals()['{}_Day_Forecasts_High_Res'.format(days)] = pd.DataFrame(pairs, columns=['Datetime', 'High Resolution Forecast (m^3/s)'])
        globals()['{}_Day_Forecasts_High_Res'.format(days)].set_index('Datetime', inplace=True)
        # Adjusting the time zone of the forecast (originally in UTC) to the local time

        globals()['{}_Day_Forecasts_High_Res'.format(days)].index = globals()['{}_Day_Forecasts_High_Res'.format(days)].index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        globals()['{}_Day_Forecasts_High_Res'.format(days)].to_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized/{2}_Day_Forecasts_High_Res.csv'.format(country, comid, days))

day_forecast = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15']
day_forecast_HR = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

for comid in comids:

    print(comid)

    if not os.path.isdir("/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/daily".format(country, comid)):
        os.makedirs("/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/daily".format(country, comid))

    Initialization = pd.read_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized/Initialization.csv'.format(country, comid), index_col=0)
    Initialization.index = pd.to_datetime(Initialization.index)
    Initialization_daily = Initialization.groupby(Initialization.index.strftime("%Y-%m-%d")).mean()
    Initialization_daily.index = pd.to_datetime(Initialization_daily.index)
    Initialization_daily.to_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized/Initialization.csv'.format(country, comid))

    for days in day_forecast:
        globals()['{}_Day_Forecasts'.format(days)] = pd.read_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized/{2}_Day_Forecasts.csv'.format(country, comid, days), index_col=0)
        globals()['{}_Day_Forecasts'.format(days)].index = pd.to_datetime(globals()['{}_Day_Forecasts'.format(days)].index)
        globals()['{}_Day_Forecasts_daily'.format(days)] = globals()['{}_Day_Forecasts'.format(days)].groupby(globals()['{}_Day_Forecasts'.format(days)].index.strftime("%Y-%m-%d")).mean()
        globals()['{}_Day_Forecasts_daily'.format(days)].index = pd.to_datetime(globals()['{}_Day_Forecasts_daily'.format(days)].index)
        globals()['{}_Day_Forecasts_daily'.format(days)].to_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/daily/{2}_Day_Forecasts.csv'.format(country, comid, days))

    for days in day_forecast_HR:
        globals()['{}_Day_Forecasts_High_Res'.format(days)] = pd.read_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/organized/{2}_Day_Forecasts_High_Res.csv'.format(country, comid, days), index_col=0)
        globals()['{}_Day_Forecasts_High_Res'.format(days)].index = pd.to_datetime(globals()['{}_Day_Forecasts_High_Res'.format(days)].index)
        globals()['{}_Day_Forecasts_High_Res_daily'.format(days)] = globals()['{}_Day_Forecasts_High_Res'.format(days)].groupby(globals()['{}_Day_Forecasts_High_Res'.format(days)].index.strftime("%Y-%m-%d")).mean()
        globals()['{}_Day_Forecasts_High_Res_daily'.format(days)].index = pd.to_datetime(globals()['{}_Day_Forecasts_High_Res_daily'.format(days)].index)
        globals()['{}_Day_Forecasts_High_Res_daily'.format(days)].to_csv('/Volumes/GoogleDrive/My Drive/Colab Notebooks/Extract_Forecast_Files/{0}/{1}/daily/{2}_Day_Forecasts_High_Res.csv'.format(country, comid, days))
