import requests
import datetime as dt

#Get Historic Data
era_res = requests.get(
            'https://tethys.byu.edu' + '/apps/streamflow-prediction-tool/api/GetHistoricData/?watershed_name=' +
            'south_asia' + '&subbasin_name=' + 'mainland' + '&reach_id=' + '55869' + '&return_format=csv',
            headers={'Authorization': 'Token d8eeafb12975bedee8fe00db5effbdc550902d56'})

era_pairs = era_res.content.splitlines()
era_pairs.pop(0)

era_dates = []
era_values = []

for era_pair in era_pairs:
    era_dates.append(dt.datetime.strptime(era_pair.split(',')[0], '%Y-%m-%d %H:%M:%S'))
    era_values.append(float(era_pair.split(',')[1]))

print(era_dates)
print(era_values)


#Get Dates
watershed='south_asia'
subbasin='mainland'
comid='55869'

res = requests.get(
            'https://tethys.byu.edu' + '/apps/streamflow-prediction-tool/api/GetAvailableDates/?watershed_name=' +
            'south_asia' + '&subbasin_name=' + 'mainland',
            headers={'Authorization': 'Token d8eeafb12975bedee8fe00db5effbdc550902d56'})

dates = []

for date in eval(res.content):
    date_mod = date + '000'
    date_f = dt.datetime.strptime(date_mod , '%Y%m%d.%H%M').strftime('%Y-%m-%d %H:%M')

dates.append(['Select Date', dates[-1][1]])
dates.reverse()

print(dates)


#Get Forecasts
res = requests.get(
			'https://tethys.byu.edu' + '/apps/streamflow-prediction-tool/api/GetForecast/?watershed_name=' +
			'south_asia' + '&subbasin_name=' + 'mainland' + '&reach_id=' + '55869' + '&forecast_folder=' +
            '20190425' + '&return_format=csv',
            headers={'Authorization': 'Token d8eeafb12975bedee8fe00db5effbdc550902d56'})