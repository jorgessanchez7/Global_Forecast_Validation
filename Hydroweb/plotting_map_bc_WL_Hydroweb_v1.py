import time
import numpy as np
import pandas as pd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import cartopy.feature as cfeature
from cartopy.io.img_tiles import OSM
import matplotlib.patches as mpatches

stations_df = pd.read_csv('E:\\GEOGloWS\\Error_Metrics\\Metrics\\Metrics_GEOGloWS_v1_WL_Sat.csv')

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
fig, ax = plt.subplots(figsize=(15, 8), subplot_kw={'projection': OSM().crs})
ax.add_image(OSM(), 7)  # The second argument is the zoom level

# Add country borders
borders = cfeature.NaturalEarthFeature('cultural', 'admin_0_countries', '10m', edgecolor='black', facecolor='none')
ax.add_feature(borders, linewidth=0.25)

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

    ax.plot(lon, lat, marker='o', color=color, markersize=3.5, markeredgewidth=0.5, markeredgecolor='black', transform=ccrs.Geodetic())

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

ax.text(-35, -35, f'${text_latex}$', fontsize=12, color='white', bbox=dict(facecolor='black', alpha=0.5), transform=ccrs.Geodetic())
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

draw_scale_bar(ax, [-150, 0], 5000, ccrs.PlateCarree())


def add_north_arrow(ax, location, arrow_length=10, arrow_color='black', text_color='blue'):
    """
    Add a north arrow to the map using FancyArrowPatch for better control.

    Args:
    ax (matplotlib.axes): The axes to which the north arrow will be added.
    location (tuple): The (x, y) coordinates of the arrow base.
    arrow_length (float): The length of the arrow in degrees (ensure your map's units).
    arrow_color (str): Color of the arrow.
    text_color (str): Color of the 'N' text.
    """
    # Create the arrow
    arrow = mpatches.FancyArrowPatch((location[0], location[1]),
                                     (location[0], location[1] + arrow_length),
                                     transform=ccrs.PlateCarree(),  # ensure correct transform
                                     arrowstyle='-|>',
                                     mutation_scale=40,  # controls the size of the arrow head
                                     color=arrow_color,
                                     linewidth=5)  # set the line width

    ax.add_patch(arrow)

    # Add 'N' label to the arrow
    ax.text(location[0], location[1] + arrow_length + 0.02, r'$\textbf{N}$',  # slight offset for text
            horizontalalignment='center',
            verticalalignment='bottom',
            fontsize=20,
            fontweight='bold',
            color=text_color,
            transform=ccrs.PlateCarree())

def find_north_arrow_location(ax):
    """Determine a good location for the north arrow based on current view limits."""
    xmin, xmax, ymin, ymax = ax.get_extent(ccrs.PlateCarree())
    # Place the north arrow at 50% of the height and 40% from the left of the map extent
    x_location = xmin + (xmax - xmin) * 0.35
    y_location = ymin + (ymax - ymin) * 0.55
    return (x_location, y_location)

# Example usage of the north arrow function
north_arrow_location = find_north_arrow_location(ax)


plt.legend(title=r'$\textbf{KGE Categories}$', loc='lower left', fontsize=8)

plt.savefig('E:\\GEOGloWS\\Error_Metrics\\Maps\\Metrics_GEOGloWS_v1_WL_Sat.png', dpi=400, bbox_inches='tight', pad_inches=0)
plt.savefig('E:\\GEOGloWS\\Error_Metrics\\Maps\\Metrics_GEOGloWS_v1_WL_Sat.pdf', format='pdf', dpi=400, bbox_inches='tight', pad_inches=0)
#plt.show()