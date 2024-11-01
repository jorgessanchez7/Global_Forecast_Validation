'''Getting real time observed data'''
import os
import pytz
import json
import calendar
import requests
import xmltodict
import pandas as pd
import datetime as dt
import xml.etree.ElementTree as ET
from dateutil.relativedelta import relativedelta

import warnings
warnings.filterwarnings('ignore')

codEstaciones = [87160000, 87380000, 87382000, 87399000, 85900000, 86895000]
comids_v1 = ['9121803', '9122828', '9122828', '9123253', '9123251', '9122638']
comids_v2 = ['640466216', '640486971', '640485823', '640455882', '640534219', '640537656']

def fetch_real_time_discharge (station_code):
    # Set the timezone to Brazil/East
    tz = pytz.timezone('Brazil/East')
    today = dt.datetime.now(tz)
    start_date = today - relativedelta(months=12)

    # Format end date
    end_day = f"{today.day:02d}"
    end_month = f"{today.month:02d}"
    end_year = str(today.year)

    # Format start date
    start_day = f"{start_date.day:02d}"
    start_month = f"{start_date.month:02d}"
    start_year = str(start_date.year)

    # Construct the URL for the data request
    url = f'http://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos?codEstacao={station_code}&DataInicio={start_day}/{start_month}/{start_year}&DataFim={end_day}/{end_month}/{end_year}'
    #print(url)

    # Make the request and parse the XML
    data = requests.get(url).content
    sites_dict = xmltodict.parse(data)
    sites_json = json.loads(json.dumps(sites_dict))

    # Extract the data
    data_records = sites_json["DataTable"]["diffgr:diffgram"]["DocumentElement"]["DadosHidrometereologicos"]

    # Prepare lists for discharge values and timestamps
    list_val_vazao = []
    list_date_vazao = []

    for record in data_records:
        list_val_vazao.append(record["Vazao"])
        list_date_vazao.append(record["DataHora"])

    # Create a DataFrame
    pairs = list(zip(list_date_vazao, list_val_vazao))
    observed_rt = pd.DataFrame(pairs, columns=['Datetime', 'Streamflow (m3/s)'])
    observed_rt.set_index('Datetime', inplace=True)
    observed_rt.index = pd.to_datetime(observed_rt.index)
    observed_rt.dropna(inplace=True)
    observed_rt.sort_index(inplace=True, ascending=True)

    return observed_rt

def fetch_historical_discharge(station_code):
    params = {
        'codEstacao': station_code,
        'dataInicio': '01/01/1900',
        'dataFim': '31/12/2024',
        'tipoDados': '3',
        'nivelConsistencia': ''
    }
    data_types = {'3': ['Vazao{:02}']}

    response = requests.get('http://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroSerieHistorica', params=params, timeout=120.0, verify=False)
    tree = ET.ElementTree(ET.fromstring(response.content))
    root = tree.getroot()
    df = []

    for month in root.iter('SerieHistorica'):
        code = month.find('EstacaoCodigo').text
        code = f'{int(code):08}'
        consist = int(month.find('NivelConsistencia').text)
        date = pd.to_datetime(month.find('DataHora').text, dayfirst=True)
        last_day = calendar.monthrange(date.year, date.month)[1]
        month_dates = pd.date_range(date, periods=last_day, freq='D')
        data = []
        list_consist = []

        for i in range(last_day):
            value = data_types['3'][0].format(i + 1)
            try:
                data.append(float(month.find(value).text))
                list_consist.append(consist)
            except (TypeError, AttributeError):
                data.append(None)
                list_consist.append(consist)

        index_multi = list(zip(month_dates, list_consist))
        index_multi = pd.MultiIndex.from_tuples(index_multi, names=["Datetime", "Consistence"])
        df.append(pd.DataFrame({code: data}, index=index_multi))

    df = pd.concat(df)
    df = df.sort_index()

    # Filter duplicates and mean over days
    drop_index = df.reset_index(level=1, drop=True).index.duplicated(keep='last')
    df = df[~drop_index]
    df = df.reset_index(level=1, drop=True)
    df.columns = ['Streamflow (m3/s)']
    observed_df = df.groupby(df.index.strftime("%Y-%m-%d")).mean()
    observed_df.index = pd.to_datetime(observed_df.index)
    observed_df.dropna(inplace=True)

    observed_df.index.name = 'Datetime'

    return observed_df

for station_code, comid_v1, comid_v2 in zip(codEstaciones, comids_v1, comids_v2):

    print(station_code, ' - ', comid_v1, ' - ', comid_v2)

    if not os.path.isdir("/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}".format(comid_v1)):
        os.makedirs("/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}".format(comid_v1))

    if not os.path.isdir("/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}".format(comid_v2)):
        os.makedirs("/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}".format(comid_v2))

    observed_rt = fetch_real_time_discharge(station_code)
    observed_rt.index = pd.to_datetime(observed_rt.index)
    observed_rt.index = observed_rt.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    observed_rt.index = pd.to_datetime(observed_rt.index)
    observed_rt['Streamflow (m3/s)'] = pd.to_numeric(observed_rt['Streamflow (m3/s)'], errors='coerce')

    observed_rt.index = observed_rt.index.tz_localize('Brazil/East')
    observed_rt.index = observed_rt.index.tz_convert('UTC')

    observed_rt = observed_rt.groupby(observed_rt.index.floor('H')).mean()
    observed_rt.index = pd.to_datetime(observed_rt.index)
    observed_rt.index = observed_rt.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    observed_rt.index = pd.to_datetime(observed_rt.index)

    full_hourly_index = pd.date_range(start=observed_rt.index.min(), end=observed_rt.index.max(), freq='H')
    observed_rt = observed_rt.reindex(full_hourly_index)

    observed_rt.index.name = 'Datetime'

    observed_rt = observed_rt.loc['2024-04-10 00:00':'2024-08-10 00:00']

    #observed_rt.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Brazil/Real_Time_Data/{}_RT.csv'.format(codEstacion))
    observed_rt.to_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}/{1}_RT.csv'.format(comid_v1, station_code))
    observed_rt.to_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}/{1}_RT.csv'.format(comid_v2, station_code))

for station_code, comid_v1, comid_v2 in zip(codEstaciones, comids_v1, comids_v2):

  print(station_code, ' - ', comid_v1, ' - ', comid_v2)

  observed_df = fetch_historical_discharge(station_code)
  observed_df.index = pd.to_datetime(observed_df.index)
  observed_df.index = observed_df.index.to_series().dt.strftime("%Y-%m-%d")
  observed_df.index = pd.to_datetime(observed_df.index)

  full_daily_index = pd.date_range(start=observed_df.index.min(), end=observed_df.index.max(), freq='D')
  observed_df = observed_df.reindex(full_daily_index)

  observed_df.index.name = 'Datetime'

  observed_df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}/{1}_HIS.csv".format(comid_v1, station_code))
  observed_df.to_csv("/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/Recent_Forecast/{0}/{1}_HIS.csv".format(comid_v2, station_code))