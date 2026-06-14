"""
init_db.py
----------
Inicializa la base de datos SQLite a partir del CSV de metadatos
(SATMA_GEOGLOWS.csv). Solo crea las tablas e inserta los metadatos;
no descarga datos. Para la carga histórica inicial usar `update_data.py`
con la opción --initial.

Ejecutar una sola vez:
    python init_db.py
"""
import csv
import sys

import config
from utils import get_connection, setup_logger


SCHEMA = """
-- Metadatos de cada estación.
CREATE TABLE IF NOT EXISTS estaciones (
    idEstacion        INTEGER PRIMARY KEY,
    codEstacion       TEXT,
    Estacion          TEXT,
    Latitud           REAL,
    Longitud          REAL,
    Altitud           REAL,
    Ubicacion         TEXT,
    Zona              INTEGER,
    FechaInstalacion  TEXT,
    Propietario       TEXT,
    Tipo              TEXT,
    TypeDB            TEXT,

    -- Flags de variables disponibles (del CSV original)
    temperature        INTEGER,
    realPrecipitation  INTEGER,
    humidity           INTEGER,
    radiation          INTEGER,
    radiationUV        INTEGER,
    presure            INTEGER,
    windSpeed          INTEGER,
    windDirection      INTEGER,
    realETO            INTEGER,
    level              INTEGER,
    riverFlow          INTEGER,

    -- Estado calculado por update_data.py
    estado              TEXT DEFAULT 'DESCONOCIDO',  -- ACTIVA | INACTIVA | DESCONOCIDO
    ultima_actualizacion TEXT,                       -- cuándo se intentó actualizar
    ultima_observacion   TEXT                        -- timestamp del dato más reciente recibido
);

-- Datos en formato largo: una fila por (estación, variable, instante)
CREATE TABLE IF NOT EXISTS mediciones (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    idEstacion  INTEGER NOT NULL,
    variable    TEXT    NOT NULL,
    timestamp   TEXT    NOT NULL,
    valor       REAL    NOT NULL,
    UNIQUE(idEstacion, variable, timestamp),
    FOREIGN KEY (idEstacion) REFERENCES estaciones(idEstacion)
);

CREATE INDEX IF NOT EXISTS idx_mediciones_est_var_ts
    ON mediciones(idEstacion, variable, timestamp);
CREATE INDEX IF NOT EXISTS idx_mediciones_ts
    ON mediciones(timestamp);

-- Bitácora de cada llamada a la API
CREATE TABLE IF NOT EXISTS log_actualizaciones (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha_ejecucion    TEXT NOT NULL,
    idEstacion         INTEGER NOT NULL,
    variable           TEXT NOT NULL,
    status             TEXT NOT NULL,     -- success | error | no_data
    registros_nuevos   INTEGER DEFAULT 0,
    mensaje            TEXT
);

CREATE INDEX IF NOT EXISTS idx_log_fecha ON log_actualizaciones(fecha_ejecucion);
"""


def _to_int_bool(v: str) -> int:
    return 1 if str(v).strip().upper() == "TRUE" else 0


def main() -> int:
    log = setup_logger()

    if not config.STATIONS_CSV.exists():
        log.error("No encuentro el CSV de metadatos en %s", config.STATIONS_CSV)
        return 1

    conn = get_connection()
    try:
        # 1. Crear esquema
        conn.executescript(SCHEMA)
        log.info("Esquema creado / verificado en %s", config.DB_PATH)

        # 2. Cargar metadatos del CSV (detección tolerante de encoding)
        filas = None
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                with open(config.STATIONS_CSV, newline="", encoding=enc) as f:
                    reader = csv.DictReader(f)
                    filas = list(reader)
                log.info("CSV leído con encoding=%s", enc)
                break
            except UnicodeDecodeError:
                continue
        if filas is None:
            log.error("No pude decodificar el CSV con ningún encoding conocido.")
            return 1

        cols = config.METADATA_COLUMNS + config.VARIABLES
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(cols)
        sql = (
            f"INSERT OR REPLACE INTO estaciones ({col_names}) "
            f"VALUES ({placeholders})"
        )

        insertados = 0
        for row in filas:
            try:
                values = []
                for c in config.METADATA_COLUMNS:
                    v = row.get(c, "").strip()
                    if c in ("idEstacion", "Zona"):
                        values.append(int(v) if v else None)
                    elif c in ("Latitud", "Longitud", "Altitud"):
                        try:
                            values.append(float(v))
                        except (TypeError, ValueError):
                            values.append(None)
                    else:
                        values.append(v or None)
                for v in config.VARIABLES:
                    values.append(_to_int_bool(row.get(v, "FALSE")))
                conn.execute(sql, values)
                insertados += 1
            except Exception as e:
                log.warning("Fila descartada (%s): %s", row.get("codEstacion"), e)

        conn.commit()
        log.info("Estaciones cargadas: %d", insertados)
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
