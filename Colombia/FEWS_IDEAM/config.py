"""
Configuración del sistema de almacenamiento FEWS IDEAM.
Editar las rutas y parámetros según tu entorno.
"""
from pathlib import Path

# === Rutas ===
# Carpeta de trabajo. Cámbiala a donde quieras que vivan los datos.
# Todo lo demás (data/, logs/, exports/, CSV) cuelga de aquí.
BASE_DIR = Path(r"C:\Users\jsanchez\Downloads\fews_db")

DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
EXPORT_DIR = BASE_DIR / "exports"

for d in (DATA_DIR, LOGS_DIR, EXPORT_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "fews.db"

# CSV maestro de estaciones (lo produces con build_stations_csv.py y lo lee init_db.py).
# Si quieres el CSV en otra carpeta distinta de BASE_DIR, ponlo absoluto:
#   STATIONS_CSV = Path(r"D:\catalogos\FEWS_estaciones.csv")
STATIONS_CSV = BASE_DIR / "FEWS_estaciones.csv"

# === API ===
BASE_URL = "https://fews.ideam.gov.co/visorfews/data"

# Endpoints con listas de estaciones (orden = prioridad para resolver
# conflictos cuando una columna aparece con valor distinto en varios)
STATION_ENDPOINTS = [
    ("H",       f"{BASE_URL}/ReporteTablaEstaciones.json"),
    ("Hsim",    f"{BASE_URL}/ReporteTablaEstacionesHsim.json"),
    ("Q",       f"{BASE_URL}/ReporteTablaEstacionesQ.json"),
    ("Qsim",    f"{BASE_URL}/ReporteTablaEstacionesQsim.json"),
    ("Calidad", f"{BASE_URL}/ReporteTablaEstacionesCalidad.json"),
]

# Plantillas de URL de series de tiempo
SERIES_URL = {
    "H": f"{BASE_URL}/series/jsonH/{{id}}.json",   # Hobs + Hsen
    "Q": f"{BASE_URL}/series/jsonQ/{{id}}.json",   # Qobs + Qsen
}

# Cliente HTTP
HTTP_VERIFY_SSL   = False   # el certificado del IDEAM falla en algunas máquinas
HTTP_TIMEOUT      = 30
HTTP_MAX_RETRIES  = 3
HTTP_RETRY_BACKOFF = 5      # segundos entre reintentos
HTTP_SLEEP_BETWEEN_CALLS = 0.3
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

# === Lógica de estado ===
# La API retiene ~1 mes, por eso 30 días.
INACTIVITY_THRESHOLD_DAYS = 30

# === Columnas estáticas que conservamos en la tabla `estaciones` ===
# Las columnas volátiles (ultimonivelsen, ultimoqsen, Estado, etc.) NO se
# guardan: el dato más reciente se deriva de la tabla `mediciones`.
STATIC_COLUMNS = [
    "id",            # PK
    "nombre",
    "lng",
    "lat",
    "altitud",
    "corriente",
    "zona",
    "subzona",
    "cenpoblado",
    "municipio",
    "depart",
    "ctg",
    "cotacero",
    "areaaferete",
    "maxnivel",      # solo en Hsim
    "maxcaudal",     # solo en Qsim
    "uroja",
    "unaranja",
    "uamarilla",
    "ubajos",
    "umaxhis",
    "subred",        # solo en Calidad
]

# Variables que descargamos por estación. Cada una con sus dos sub-series.
SERIES_VARIABLES = {
    "H": ("Hobs", "Hsen"),   # observado, sensor automático
    "Q": ("Qobs", "Qsen"),
}
