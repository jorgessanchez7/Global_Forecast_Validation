import pandas as pd

def summarize_station_csv(obs_path, sim_path):
    """
    Procesa un archivo CSV de una estación satelital y devuelve un diccionario
    con resumen de fechas y cobertura mensual por año.

    Parámetros:
    -----------
    - csv_path (str): Ruta al archivo .csv con columnas 'datetime', 'waterlevel'

    Retorna:
    --------
    - dict con claves: start_date, end_date, total_count,
      jan_best_year, jan_count, feb_best_year, feb_count, ..., dec_best_year, dec_count
    """
    df = pd.read_csv(obs_path, parse_dates=['Datetime'])
    df2 = pd.read_csv(sim_path)
    df2.rename(columns={df2.columns[0]: 'Datetime'}, inplace=True)
    df2['Datetime'] = pd.to_datetime(df2['Datetime'])

    # Renombrar columnas para seguridad
    obs_df = df.rename(columns={df.columns[1]: 'obs'})
    sim_df = df2.rename(columns={df2.columns[1]: 'sim'})

    obs_df['month'] = obs_df['Datetime'].dt.month
    sim_df['month'] = sim_df['Datetime'].dt.month

    merged_df = pd.merge(obs_df[['Datetime', 'month', 'obs']], sim_df[['Datetime', 'sim']], on='Datetime', how='inner')

    summary = {
        'start_date': df['Datetime'].min().date(),
        'end_date': df['Datetime'].max().date(),
        'total_count': len(df)
    }

    # Procesar cada mes
    for month in range(1, 13):
        mon = pd.to_datetime(f'2020-{month:02d}-01').strftime('%b').lower()

        # Conteo observaciones por mes
        obs_month = obs_df[obs_df['month'] == month]
        summary[f'obs_count_{mon}'] = len(obs_month)

        # Correlación mensual (si hay datos cruzados)
        merged_month = merged_df[merged_df['month'] == month].dropna()
        if len(merged_month) > 1:
            summary[f'corr_{mon}'] = merged_month['obs'].corr(merged_month['sim'])
        else:
            summary[f'corr_{mon}'] = None

    return summary

#stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Metrics_GEOGloWS_v1_sat_WL.csv')
stations_pd = pd.read_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Metrics_GEOGloWS_v2_sat_WL.csv')

for i, row in stations_pd.iterrows():
    obs_file = row['Code']
    obs_path = "/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Observed_Hydroweb/{0}.csv".format(obs_file)
    #sim_file = row['Code'] + '-' + str(row['COMID_v1'])
    sim_file = row['Code'] + '-' + str(row['COMID_v2'])
    #sim_path = "/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/DWLT/GEOGLOWS_v1/{0}_WL.csv".format(sim_file)
    sim_path = "/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/DWLT/GEOGLOWS_v2/{0}_WL.csv".format(sim_file)

    try:
        summary = summarize_station_csv(obs_path, sim_path)
        for key, value in summary.items():
            stations_pd.at[i, key] = value
    except Exception as e:
        print(f"⚠️ Error procesando {obs_path}: {e}")

print(stations_pd)

#stations_pd.to_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Metrics_GEOGloWS_v1_sat_WL.csv', index=False)
stations_pd.to_csv('/Users/grad/Library/CloudStorage/GoogleDrive-jsanchez@aquaveo.com/My Drive/Personal_Files/Post_Doc/Hydroweb/Metrics_GEOGloWS_v2_sat_WL.csv', index=False)