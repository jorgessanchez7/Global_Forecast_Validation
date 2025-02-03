import pandas as pd
import geopandas as gpd

# Función para reemplazar caracteres especiales
def reemplazar_caracteres(texto):
    caracteres_especiales = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'ñ': 'n', 'Ñ': 'N', '√Å': 'A', '√©': 'e', '√â': 'E',
        '√ç': 'I', '√≥': 'o', '√ì': 'O', '√ì': 'O', '√Ñ': 'O',
        '√ö': 'U', '√ú': 'U', '√ë': 'N'
    }
    for original, reemplazo in caracteres_especiales.items():
        texto = texto.replace(original, reemplazo)
    return texto

# Cargar el shapefile

shape_files = ['Estaciones_CentralesHidroenergeticas', 'Estaciones_Hidrologicas', 'Estaciones_Historicas']


for shape in shape_files:

    ruta_shapefile = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama/Datos_Espaciales/{0}.shp".format(shape)
    gdf = gpd.read_file(ruta_shapefile)

    # Reemplazar caracteres especiales en los nombres de las columnas
    gdf.columns = [reemplazar_caracteres(col) for col in gdf.columns]

    # Exportar la tabla de atributos a CSV
    ruta_csv = "/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama/Datos_Espaciales/{0}.csv".format(shape)
    gdf.to_csv(ruta_csv, index=False, encoding="utf-8")

    print(f"Tabla de atributos exportada a: {ruta_csv}")
