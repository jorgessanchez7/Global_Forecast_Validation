import os
import zipfile
import py_hydroweb

def download_hydroweb(api_key: str, basket_name: str, collections: list, bbox: list, zip_filename: str, extract_folder: str) -> str:
    """
    Descarga un archivo ZIP con datos de nivel de agua desde HydroWeb
    y lo extrae en una carpeta específica.

    Parámetros:
    -----------
    - api_key (str): Clave API para acceder a HydroWeb.
    - basket_name (str): Nombre del "basket" de descarga.
    - collections (list): Lista de colecciones a incluir.
    - bbox (list): Coordenadas de la caja delimitadora [minlon, minlat, maxlon, maxlat].
    - zip_filename (str): Nombre del archivo ZIP que se va a descargar.
    - extract_folder (str): Carpeta donde se extraerán los archivos.

    Retorna:
    --------
    - Ruta a la carpeta de extracción.
    """
    # Crear cliente y basket de descarga
    client = py_hydroweb.Client(api_key=api_key)
    basket = py_hydroweb.DownloadBasket(basket_name)
    for collection in collections:
        basket.add_collection(collection, bbox=bbox)

    # Enviar solicitud y descargar ZIP
    client.submit_and_download_zip(basket, zip_filename=zip_filename)

    # Crear carpeta de destino si no existe
    os.makedirs(extract_folder, exist_ok=True)

    # Extraer contenido del ZIP
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)

    # Eliminar el ZIP después de extraer
    os.remove(zip_filename)
    print(f"Datos descargados y extraídos en: {extract_folder}")
    return extract_folder

# Ejemplo de uso
if __name__ == "__main__":
    API_KEY = "kFb4XBaZX7d4RnWccF4jJugGQpAyeWvbLS1u09ngrHBqEF4gco"
    BASKET_NAME = "mi_descarga_hydroweb"
    COLLECTIONS = ["HYDROWEB_RIVERS_RESEARCH", "HYDROWEB_RIVERS_OPE"]
    BBOX = [-180, -90, 180, 90]
    ZIP_FILENAME = "hydroweb_data.zip"
    EXTRACT_FOLDER = "hydroweb_download"

    download_hydroweb(API_KEY, BASKET_NAME, COLLECTIONS, BBOX, ZIP_FILENAME, EXTRACT_FOLDER)
