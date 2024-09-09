'''Getting real time observed data'''
import pytz
import json
import requests
import xmltodict
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta

import warnings
warnings.filterwarnings('ignore')

codEstaciones = [87160000, 87380000, 87382000, 87399000, 85900000, 86895000]

for codEstacion in codEstaciones:

    print(codEstacion)

    tz = pytz.timezone('Brazil/East')
    hoy = dt.datetime.now(tz)
    ini_date = hoy - relativedelta(months=12)
    anyo = hoy.year
    mes = hoy.month
    dia = hoy.day

    if dia < 10:
        DD = '0' + str(dia)
    else:
        DD = str(dia)

    if mes < 10:
        MM = '0' + str(mes)
    else:
        MM = str(mes)

    YYYY = str(anyo)

    ini_anyo = ini_date.year
    ini_mes = ini_date.month
    ini_dia = ini_date.day

    if ini_dia < 10:
        ini_DD = '0' + str(ini_dia)
    else:
        ini_DD = str(ini_dia)

    if ini_mes < 10:
        ini_MM = '0' + str(ini_mes)
    else:
        ini_MM = str(ini_mes)

    ini_YYYY = str(ini_anyo)

    url = 'http://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos?codEstacao={0}&DataInicio={1}/{2}/{3}&DataFim={4}/{5}/{6}'.format(codEstacion, ini_DD, ini_MM, ini_YYYY, DD, MM, YYYY)

    print(url)

    datos = requests.get(url).content
    sites_dict = xmltodict.parse(datos)
    sites_json_object = json.dumps(sites_dict)
    sites_json = json.loads(sites_json_object)

    datos_c = sites_json["DataTable"]["diffgr:diffgram"]["DocumentElement"]["DadosHidrometereologicos"]

    list_val_vazao = []
    list_date_vazao = []

    for dat in datos_c:
        list_val_vazao.append(dat["Vazao"])
        list_date_vazao.append(dat["DataHora"])

    pairs = [list(a) for a in zip(list_date_vazao, list_val_vazao)]
    observed_rt = pd.DataFrame(pairs, columns=['Datetime', 'Streamflow (m3/s)'])
    observed_rt.set_index('Datetime', inplace=True)
    observed_rt.index = pd.to_datetime(observed_rt.index)
    observed_rt.dropna(inplace=True)

    # observed_rt.index = observed_rt.index.tz_localize('UTC')
    observed_rt = observed_rt.dropna()
    observed_rt.sort_index(inplace=True, ascending=True)

    observed_rt.to_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Brazil/Real_Time_Data/{}_RT.csv'.format(codEstacion))