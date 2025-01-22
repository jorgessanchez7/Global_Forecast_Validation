import time
import numpy as np
import pandas as pd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import cartopy.feature as cfeature
from cartopy.io.img_tiles import OSM
import matplotlib.patches as mpatches

stations_df = pd.read_csv('Metrics_GEOGloWS_v1_WL.csv')

selected_countries = ["Colombia", "Ecuador", "Brazil", "Peru"]
stations_df = stations_df[stations_df['Country'].isin(selected_countries)]

# Example station data (longitudes, latitudes, and KGE values)
lons = stations_df['Longitude'].to_list()
lats = stations_df['Latitude'].to_list()
kge_values = stations_df['Corrected KGE'].to_list()

# Calculate median and IQR
number_of_points = len(kge_values)
median_kge = round(np.median(kge_values), 2)
p25 = round(np.percentile(kge_values, 25), 2)
p75 = round(np.percentile(kge_values, 75), 2)

plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'

# Throttle to prevent too many requests
time.sleep(1)

# Initialize the plot with OpenStreetMap tiles
fig, ax = plt.subplots(figsize=(8, 15), subplot_kw={'projection': OSM().crs})
ax.add_image(OSM(), 9)  # The second argument is the zoom level

# Add country borders
borders = cfeature.NaturalEarthFeature('cultural', 'admin_0_countries', '10m', edgecolor='black', facecolor='none')
ax.add_feature(borders, linewidth=0.3)

# Plotting
for lon, lat, kge in zip(lons, lats, kge_values):
    if kge < -0.41:
        color = 'red'  # < -0.41
    elif -0.41 <= kge < 0.0:
        color = 'orange'  # -0.41 to 0.00
    elif 0.0 <= kge < 0.5:
        color = 'yellow'  # 0.00 to 0.50
    elif 0.5 <= kge < 0.75:
        color = 'lightgreen'  # 0.50 to 0.75
    else:
        color = 'green'  # 0.75 to 1.00

    ax.plot(lon, lat, marker='o', color=color, markersize=3.0, markeredgewidth=0.5, markeredgecolor='black', transform=ccrs.Geodetic())

# Custom legend
#legend_labels = ['< -0.41', '-0.41 — 0.00', '0.00 — 0.50', '0.50 — 0.75', '0.75 — 1.00']
legend_labels = [r'$KGE < -0.41$', r'$-0.41 \leq KGE < 0.00$', r'$0.00 \leq KGE < 0.50$', r'$0.50 \leq KGE < 0.75$', r'$0.75 \leq KGE \leq 1.00$']
legend_colors = ['red', 'orange', 'yellow', 'lightgreen', 'green']
for color, label in zip(legend_colors, legend_labels):
    ax.plot([], [], marker='o', color=color, label=label, linestyle='None', markersize=5, markeredgewidth=0.5, markeredgecolor='black')

# Display median and IQR on the plot
#ax.text(-35, -30, f'n: {number_of_points}\nMedian KGE: {median_kge}\nIQR KGE: ({p25}, {p75})', fontsize=12, color='white',
#        bbox=dict(facecolor='black', alpha=0.5), transform=ccrs.Geodetic())

# Example LaTeX formatted text
text_latex = (rf'n: {number_of_points}\\'
              rf'\text{{Median KGE}}: {median_kge}\\'
              rf'\text{{IQR KGE}}: ({p25}, {p75})')

ax.text(-48, -30, f'${text_latex}$', fontsize=12, color='white', bbox=dict(facecolor='black', alpha=0.5), transform=ccrs.Geodetic())
#ax.text(0.5, 0.5, f'${text_latex}$', fontsize=12, color='black', verticalalignment='center')

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

# Assuming lons and lats are your lists of longitude and latitude values
min_lon, max_lon = min(lons), max(lons)
min_lat, max_lat = min(lats), max(lats)

# Optionally, add a buffer to each side
buffer = 1  # buffer in degrees, adjust as necessary
map_extent = [min_lon - buffer, max_lon + buffer, min_lat - buffer, max_lat + buffer]

draw_scale_bar(ax, [-50, 5], 1500, ccrs.PlateCarree())

#plt.legend(title=r'KGE Categories', loc='lower left', fontsize=8)
plt.legend(title=r'$\textbf{KGE Categories}$', loc='lower left', fontsize=8)
#plt.title('Global Distribution of KGE Values for Hydrological Stations', fontsize=15)
plt.savefig('/Users/grad/Library/CloudStorage/OneDrive-BrighamYoungUniversity/National_Water_Level_Forecast/Plots/Metrics_GEOGLOWS_v1_bc_WL_Amazonia.png', dpi=400, bbox_inches='tight', pad_inches=0)
#plt.show()