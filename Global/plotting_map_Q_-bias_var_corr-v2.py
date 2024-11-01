import time
import numpy as np
import pandas as pd
from tqdm import tqdm
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import cartopy.feature as cfeature
from cartopy.io.img_tiles import OSM
import matplotlib.patches as mpatches

# Plotting
def plot_bias_map(ax, stations_df, title):
    lons = stations_df['Longitude'].to_list()
    lats = stations_df['Latitude'].to_list()
    #bias_values = stations_df['Bias'].to_list() #Original
    bias_values = stations_df['Bias Corrected'].to_list() #Corrected

    # Calcular estadísticas
    number_of_points = len(bias_values)
    median_bias = round(np.nanmedian(bias_values), 2)
    p25_bias = round(np.nanpercentile(bias_values, 25), 2)
    p75_bias = round(np.nanpercentile(bias_values, 75), 2)

    # Throttle to prevent too many requests
    #time.sleep(1)

    # Cargar el mapa base de OpenStreetMap
    ax.add_image(OSM(), 6)  # El segundo argumento es el nivel de zoom

    # Añadir fronteras de países
    ax.add_feature(borders, linewidth=0.25)

    # Plotear los puntos con colores según los valores de KGE
    for lon, lat, bias in zip(lons, lats, bias_values):
        if bias > 2.0:
            color = 'red'
        elif 0.0 <= bias < 0.2:
            color = 'red'
        elif 1.4 <= bias < 2.0:
            color = 'orange'
        elif 0.2 <= bias < 0.6:
            color = 'orange'
        elif 1.2 <= bias < 1.4:
            color = 'yellow'
        elif 0.6 <= bias < 0.8:
            color = 'yellow'
        elif 1.1 <= bias < 1.2:
            color = 'lightgreen'
        elif 0.8 <= bias < 0.9:
            color = 'lightgreen'
        else:
            color = 'green'
        ax.plot(lon, lat, marker='o', color=color, markersize=2.5, markeredgewidth=0.5, markeredgecolor='black', transform=ccrs.Geodetic())

    # Añadir leyenda personalizada
    legend_labels = [r'$0.0 \leq \beta < 0.2$', r'$0.2 \leq \beta < 0.6$', r'$0.6 \leq \beta < 0.8$', r'$0.8 \leq \beta < 0.9$', r'$0.9 \leq \beta \leq 1.1$', r'$1.1 < \beta \leq 1.2$', r'$1.2 < \beta \leq 1.4$', r'$1.4 < \beta \leq 2.0$', r'$\beta > 2.0$']
    legend_colors = ['red', 'orange', 'yellow', 'lightgreen', 'green', 'lightgreen', 'yellow', 'orange', 'red']
    for color, label in zip(legend_colors, legend_labels):
        ax.plot([], [], marker='o', color=color, label=label, linestyle='None', markersize=4, markeredgewidth=0.5, markeredgecolor='black')

    # Añadir texto con estadísticas
    # Example LaTeX formatted text
    text_latex = (rf'n: {number_of_points}\\'
                  rf'\text{{Median}}: {median_bias}\\'
                  rf'\text{{IQR}}: ({p25_bias}, {p75_bias})')

    ax.text(-35, -25, f'${text_latex}$', fontsize=12, color='white', bbox=dict(facecolor='black', alpha=0.5), transform=ccrs.Geodetic())

    # Assuming lons and lats are your lists of longitude and latitude values
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    # Optionally, add a buffer to each side
    buffer = 1  # buffer in degrees, adjust as necessary
    map_extent = [min_lon - buffer, max_lon + buffer, min_lat - buffer, max_lat + buffer]
    ax.set_extent(map_extent, crs=ccrs.PlateCarree())

    draw_scale_bar(ax, [50, 0], 5000, ccrs.PlateCarree())
    ax.legend(title=r'$\textbf{Bias}$', loc='lower left', fontsize=8)
    # Título del mapa
    ax.set_title(title, fontsize=15)

def plot_variability_map(ax, stations_df, title):
    lons = stations_df['Longitude'].to_list()
    lats = stations_df['Latitude'].to_list()
    #variability_values = stations_df['Variability'].to_list() #Original
    variability_values = stations_df['Corrected Variability'].to_list()  # Corrected

    # Calcular estadísticas
    number_of_points = len(variability_values)
    median_variability = round(np.nanmedian(variability_values), 2)
    p25_variability = round(np.nanpercentile(variability_values, 25), 2)
    p75_variability = round(np.nanpercentile(variability_values, 75), 2)

    # Throttle to prevent too many requests
    #time.sleep(1)

    # Cargar el mapa base de OpenStreetMap
    #ax.add_image(OSM(), 6)  # El segundo argumento es el nivel de zoom

    ax.add_feature(borders, linewidth=0.25)

    # Plotear los puntos con colores según los valores de KGE
    for lon, lat, variability in zip(lons, lats, variability_values):
        if variability > 2.0:
            color = 'red'
        elif 0.0 <= variability < 0.2:
            color = 'red'
        elif 1.4 <= variability < 2.0:
            color = 'orange'
        elif 0.2 <= variability < 0.6:
            color = 'orange'
        elif 1.2 <= variability < 1.4:
            color = 'yellow'
        elif 0.6 <= variability < 0.8:
            color = 'yellow'
        elif 1.1 <= variability < 1.2:
            color = 'lightgreen'
        elif 0.8 <= variability < 0.9:
            color = 'lightgreen'
        else:
            color = 'green'
        ax.plot(lon, lat, marker='o', color=color, markersize=2.5, markeredgewidth=0.5, markeredgecolor='black', transform=ccrs.Geodetic())

    # Añadir leyenda personalizada
    legend_labels = [r'$0.0 \leq \gamma < 0.2$', r'$0.2 \leq \gamma < 0.6$', r'$0.6 \leq \gamma < 0.8$', r'$0.8 \leq \gamma < 0.9$', r'$0.9 \leq \gamma \leq 1.1$', r'$1.1 < \gamma \leq 1.2$', r'$1.2 < \gamma \leq 1.4$', r'$1.4 < \gamma \leq 2.0$', r'$\gamma > 2.0$']
    legend_colors = ['red', 'orange', 'yellow', 'lightgreen', 'green', 'lightgreen', 'yellow', 'orange', 'red']
    for color, label in zip(legend_colors, legend_labels):
        ax.plot([], [], marker='o', color=color, label=label, linestyle='None', markersize=4, markeredgewidth=0.5, markeredgecolor='black')

    # Añadir texto con estadísticas
    # Example LaTeX formatted text
    text_latex = (rf'n: {number_of_points}\\'
                  rf'\text{{Median}}: {median_variability}\\'
                  rf'\text{{IQR}}: ({p25_variability}, {p75_variability})')

    ax.text(-35, -25, f'${text_latex}$', fontsize=12, color='white', bbox=dict(facecolor='black', alpha=0.5), transform=ccrs.Geodetic())

    # Assuming lons and lats are your lists of longitude and latitude values
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    # Optionally, add a buffer to each side
    buffer = 1  # buffer in degrees, adjust as necessary
    map_extent = [min_lon - buffer, max_lon + buffer, min_lat - buffer, max_lat + buffer]
    ax.set_extent(map_extent, crs=ccrs.PlateCarree())

    draw_scale_bar(ax, [50, 0], 5000, ccrs.PlateCarree())
    ax.legend(title=r'$\textbf{Variability}$', loc='lower left', fontsize=8)
    # Título del mapa
    ax.set_title(title, fontsize=15)

def plot_correlation_map(ax, stations_df, title):
    lons = stations_df['Longitude'].to_list()
    lats = stations_df['Latitude'].to_list()
    #correlation_values = stations_df['Correlation'].to_list() #Original
    correlation_values = stations_df['Corrected Correlation'].to_list()  # Corrected

    # Calcular estadísticas
    number_of_points = len(correlation_values)
    median_correlation = round(np.nanmedian(correlation_values), 2)
    p25_correlation = round(np.nanpercentile(correlation_values, 25), 2)
    p75_correlation = round(np.nanpercentile(correlation_values, 75), 2)

    # Throttle to prevent too many requests
    #time.sleep(1)

    # Cargar el mapa base de OpenStreetMap
    #ax.add_image(OSM(), 6)  # El segundo argumento es el nivel de zoom

    # Añadir fronteras de países
    ax.add_feature(borders, linewidth=0.25)

    # Plotear los puntos con colores según los valores de KGE
    for lon, lat, correlation in zip(lons, lats, correlation_values):
        if correlation < 0.0:
            color = '#5e2129'
        elif 0.0 <= correlation < 0.2:
            color = 'red'
        elif 0.2 <= correlation < 0.4:
            color = 'orange'
        elif 0.4 <= correlation < 0.6:
            color = 'yellow'
        elif 0.6 <= correlation < 0.8:
            color = 'lightgreen'
        else:
            color = 'green'
        ax.plot(lon, lat, marker='o', color=color, markersize=2.5, markeredgewidth=0.5, markeredgecolor='black', transform=ccrs.Geodetic())

    # Añadir leyenda personalizada
    legend_labels = [r'$r \leq 0.0$', r'$0.0 < r \leq 0.2$', r'$0.2 < r \leq 0.4$', r'$0.4 < r \leq 0.6$', r'$0.6 < r \leq 0.8$', r'$0.8 < r \leq 1.0$']
    legend_colors = ['#5e2129', 'red', 'orange', 'yellow', 'lightgreen', 'green']
    for color, label in zip(legend_colors, legend_labels):
        ax.plot([], [], marker='o', color=color, label=label, linestyle='None', markersize=4, markeredgewidth=0.5, markeredgecolor='black')

    # Añadir texto con estadísticas
    # Example LaTeX formatted text
    text_latex = (rf'n: {number_of_points}\\'
                  rf'\text{{Median}}: {median_correlation}\\'
                  rf'\text{{IQR}}: ({p25_correlation}, {p75_correlation})')

    ax.text(-35, -25, f'${text_latex}$', fontsize=12, color='white', bbox=dict(facecolor='black', alpha=0.5), transform=ccrs.Geodetic())

    # Assuming lons and lats are your lists of longitude and latitude values
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    # Optionally, add a buffer to each side
    buffer = 1  # buffer in degrees, adjust as necessary
    map_extent = [min_lon - buffer, max_lon + buffer, min_lat - buffer, max_lat + buffer]
    ax.set_extent(map_extent, crs=ccrs.PlateCarree())

    draw_scale_bar(ax, [50, 0], 5000, ccrs.PlateCarree())
    ax.legend(title=r'$\textbf{Correlation}$', loc='lower left', fontsize=8)
    # Título del mapa
    ax.set_title(title, fontsize=15)

def draw_scale_bar(ax, location, length_km, projection):
    """ Draw a scale bar with a specified length in kilometers. """
    x, y = location
    # Convert kilometers to degrees at the given latitude (approximation)
    scale_length_deg = length_km / (111.32 * np.cos(np.radians(y)))  # 111.32 km/deg is an approximation

    # Draw the scale bar
    ax.plot([x, x + scale_length_deg], [y, y], transform=projection,
            color='black', linewidth=3)

    # Scale bar text
    ax.text(x + scale_length_deg / 2, y - 1.5, f'{length_km} km',
            horizontalalignment='center', verticalalignment='top',
            transform=projection, color='black', fontsize=12)

stations_df = pd.read_csv('Metrics_GEOGloWS_v2_Q.csv')

plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'

# Añadir fronteras de países
borders = cfeature.NaturalEarthFeature('cultural', 'admin_0_countries', '10m', edgecolor='black', facecolor='none')

fig, axs = plt.subplots(3, 1, figsize=(15, 16), subplot_kw={'projection': OSM().crs})

tile_provider = OSM()
axs[0].add_image(tile_provider, 1)
axs[1].add_image(tile_provider, 1)
axs[2].add_image(tile_provider, 1)

# Graficar el mapa para bias en la parte superior
plot_bias_map(axs[0], stations_df, 'Bias Ratio')
# Graficar el mapa para variability en la parte media
plot_variability_map(axs[1], stations_df, 'Variability Ratio')
# Graficar el mapa para correlation en la parte inferior
plot_correlation_map(axs[2], stations_df, 'Pearson Correlation')

#plt.title('The three components of the Kling-Gupta efficiency (KGE)—bias, variability, and correlation', fontsize=15)
#plt.savefig('bias_var_corr_GEOGLOWS_v2_Q.png', dpi=400, bbox_inches='tight', pad_inches=0)
#plt.savefig('bias_var_corr_GEOGLOWS_v2_Q.pdf', format='pdf', dpi=400, bbox_inches='tight', pad_inches=0)
plt.savefig('bias_var_corr_GEOGLOWS_v2_Q_bc.png', dpi=400, bbox_inches='tight', pad_inches=0)
#plt.savefig('bias_var_corr_GEOGLOWS_v2_Q_bc.pdf', format='pdf', dpi=400, bbox_inches='tight', pad_inches=0)
#plt.show()