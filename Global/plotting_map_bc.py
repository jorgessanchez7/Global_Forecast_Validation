import numpy as np
import pandas as pd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import cartopy.feature as cfeature
from cartopy.io.img_tiles import OSM
import matplotlib.patches as mpatches

plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'

# Función para dibujar el mapa con los datos de KGE
def plot_kge_map(ax, stations_df, title):
    lons = stations_df['Longitude'].to_list()
    lats = stations_df['Latitude'].to_list()
    kge_values = stations_df['Corrected KGE'].to_list()

    # Calcular estadísticas
    number_of_points = len(kge_values)
    median_kge = round(np.nanmedian(kge_values), 2)
    p25 = round(np.nanpercentile(kge_values, 25), 2)
    p75 = round(np.nanpercentile(kge_values, 75), 2)

    # Cargar el mapa base de OpenStreetMap
    ax.add_image(OSM(), 6)  # El segundo argumento es el nivel de zoom

    # Añadir fronteras de países
    borders = cfeature.NaturalEarthFeature('cultural', 'admin_0_countries', '10m',
                                           edgecolor='black', facecolor='none')
    ax.add_feature(borders, linewidth=0.25)

    # Plotear los puntos con colores según los valores de KGE
    for lon, lat, kge in zip(lons, lats, kge_values):
        if kge < -0.41:
            color = 'red'
        elif -0.41 <= kge < 0.0:
            color = 'orange'
        elif 0.0 <= kge < 0.5:
            color = 'yellow'
        elif 0.5 <= kge < 0.75:
            color = 'lightgreen'
        else:
            color = 'green'
        ax.plot(lon, lat, marker='o', color=color, markersize=3, markeredgewidth=0.5,
                markeredgecolor='black', transform=ccrs.Geodetic())

    # Añadir leyenda personalizada
    legend_labels = [r'$KGE < -0.41$', r'$-0.41 \leq KGE < 0.00$', r'$0.00 \leq KGE < 0.50$', r'$0.50 \leq KGE < 0.75$', r'$0.75 \leq KGE \leq 1.00$']
    legend_colors = ['red', 'orange', 'yellow', 'lightgreen', 'green']
    for color, label in zip(legend_colors, legend_labels):
        ax.plot([], [], marker='o', color=color, label=label, linestyle='None', markersize=6,
                markeredgewidth=0.5, markeredgecolor='black')

    # Añadir texto con estadísticas
    # Example LaTeX formatted text
    text_latex = (rf'n: {number_of_points}\\'
                  rf'\text{{Median KGE}}: {median_kge}\\'
                  rf'\text{{IQR KGE}}: ({p25}, {p75})')

    ax.text(-35, -25, f'${text_latex}$', fontsize=12, color='white', bbox=dict(facecolor='black', alpha=0.5),
            transform=ccrs.Geodetic())
    # ax.text(0.5, 0.5, f'${text_latex}$', fontsize=12, color='black', verticalalignment='center')

    # Título del mapa
    ax.set_title(title, fontsize=15)

# Leer los archivos CSV de los dos conjuntos de datos
stations_v1_df = pd.read_csv("Metrics_GEOGloWS_v1_Q.csv")
stations_v1_df.dropna(inplace=True)
stations_v2_df = pd.read_csv("Metrics_GEOGloWS_v2_Q.csv")
stations_v2_df.dropna(inplace=True)

# Crear subplots para los dos mapas
fig, axs = plt.subplots(2, 1, figsize=(15, 16), subplot_kw={'projection': OSM().crs})

# Graficar el mapa para GEOGloWS v1 en la parte superior
plot_kge_map(axs[0], stations_v1_df, 'KGE Values - Bias Crrected GEOGloWS v1')

# Graficar el mapa para GEOGloWS v2 en la parte inferior
plot_kge_map(axs[1], stations_v2_df, 'KGE Values - Bias Corrected GEOGloWS v2')

# Añadir leyenda y guardar la imagen
plt.legend(title=r'$\textbf{KGE Categories}$', loc='lower left', fontsize=8)
plt.savefig('Metrics_GEOGLOWS_v1_vs_v2_bc.png', dpi=400, pad_inches=0)
#plt.show()