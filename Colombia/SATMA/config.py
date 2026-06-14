"""
Configuración del sistema de almacenamiento SATMA.
Editar las rutas y parámetros según tu entorno.
"""
from pathlib import Path

# === Rutas ===
BASE_DIR = Path(r"C:\Users\jsanchez\Downloads\satma_db")
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
EXPORT_DIR = BASE_DIR / "exports"

# Crear directorios si no existen
for d in (DATA_DIR, LOGS_DIR, EXPORT_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "satma.db"
STATIONS_CSV = BASE_DIR / "SATMA_GEOGLOWS.csv"   # CSV original con metadatos

# === API ===
API_BASE_URL = "https://api.satma.co/api/registros"
API_TIMEOUT = 30          # segundos
API_MAX_RETRIES = 3
API_RETRY_BACKOFF = 5     # segundos entre reintentos
API_SLEEP_BETWEEN_CALLS = 0.3   # pausa cortés entre llamadas para no saturar

# Días a pedir en cada modo
DAYS_INITIAL_LOAD = 60   # carga histórica inicial (máximo permitido por la API)
DAYS_DAILY_UPDATE = 2     # actualizaciones diarias: pequeño buffer para no perder datos

# === Lógica de estado ===
# Una estación se considera ACTIVA si tiene al menos un dato en cualquier
# variable durante los últimos N días. Si no, INACTIVA.
INACTIVITY_THRESHOLD_DAYS = 180

# === Variables conocidas ===
# Estas son las columnas booleanas del CSV original que indican qué
# variables soporta cada estación.
VARIABLES = [
    "temperature",
    "realPrecipitation",
    "humidity",
    "radiation",
    "radiationUV",
    "presure",
    "windSpeed",
    "windDirection",
    "realETO",
    "level",
    "riverFlow",
]

# Columnas de metadatos del CSV original (todo lo que NO es variable)
METADATA_COLUMNS = [
    "codEstacion",
    "Estacion",
    "Latitud",
    "Longitud",
    "Altitud",
    "Ubicacion",
    "Zona",
    "FechaInstalacion",
    "Propietario",
    "Tipo",
    "idEstacion",
    "TypeDB",
]

# Unidades de cada variable (para los headers de export_csv.py)
VARIABLE_UNITS = {
    "temperature":       "°C",
    "realPrecipitation": "mm",
    "humidity":          "%",
    "radiation":         "W/m2",
    "radiationUV":       "W/m2",
    "presure":           "mm/Hg",
    "windSpeed":         "m/s",
    "windDirection":     "°",
    "realETO":           "mm",
    "level":             "cm",
    "riverFlow":         "m3/s",
}
