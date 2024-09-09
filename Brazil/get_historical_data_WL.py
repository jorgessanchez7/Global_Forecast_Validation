import requests
import calendar
import pandas as pd
import xml.etree.ElementTree as ET

import warnings
warnings.filterwarnings('ignore')

codEstaciones = [13962000]

for codEstacion in codEstaciones:

    print(codEstacion)

    params = {'codEstacao': '', 'dataInicio': '01/01/1900', 'dataFim': '31/12/2024', 'tipoDados': '', 'nivelConsistencia': ''}
    data_types = {'3': ['Vazao{:02}'], '2': ['Chuva{:02}'], '1': ['Cota{:02}']}

    params['codEstacao'] = str(codEstacion)
    params['tipoDados'] = '1'

    response = requests.get('http://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroSerieHistorica', params, timeout=120.0)
    tree = ET.ElementTree(ET.fromstring(response.content))

    root = tree.getroot()
    df = []

    for month in root.iter('SerieHistorica'):
        code = month.find('EstacaoCodigo').text
        code = f'{int(code):08}'
        consist = int(month.find('NivelConsistencia').text)
        date = pd.to_datetime(month.find('DataHora').text, dayfirst=False)
        last_day = calendar.monthrange(date.year, date.month)[1]
        month_dates = pd.date_range(date, periods=last_day, freq='D')
        data = []
        list_consist = []

        for i in range(last_day):
            value = data_types[params['tipoDados']][0].format(i + 1)
            try:
                data.append(float(month.find(value).text))
                list_consist.append(consist)
            except TypeError:
                data.append(month.find(value).text)
                list_consist.append(consist)
            except AttributeError:
                data.append(None)
                list_consist.append(consist)

        index_multi = list(zip(month_dates, list_consist))
        index_multi = pd.MultiIndex.from_tuples(index_multi, names=["Datetime", "Consistence"])
        df.append(pd.DataFrame({code: data}, index=index_multi))

    df = pd.concat(df)
    df = df.sort_index()

    drop_index = df.reset_index(level=1, drop=True).index.duplicated(keep='last')
    df = df[~drop_index]
    df = df.reset_index(level=1, drop=True)

    df.columns = ['Water Level (cm)']
    daily_df = df.groupby(df.index.strftime("%Y/%m/%d")).mean()
    daily_df.index = pd.to_datetime(daily_df.index)
    daily_df.index = daily_df.index.to_series().dt.strftime("%Y-%m-%d")
    daily_df.index = pd.to_datetime(daily_df.index)

    daily_df.to_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jorge.sanchez.geoglows@gmail.com/My Drive/Colab Notebooks/GEOGloWS_Training_Materials/Observed_Data/Brazil/{}_WL.csv'.format(codEstacion))