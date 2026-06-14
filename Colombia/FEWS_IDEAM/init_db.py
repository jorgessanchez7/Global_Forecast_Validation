"""
init_db.py
----------
Inicializa la base de datos FEWS IDEAM a partir del CSV maestro producido
por `build_stations_csv.py`.

Este script NO accede a la API. Solo:
  1. Crea las tablas (idempotente).
  2. Lee FEWS_estaciones.csv.
  3. Inserta o actualiza filas en `estaciones`.

Es directamente análogo al init_db.py del proyecto SATMA.

Ejecutar:
    python init_db.py
"""
import csv
import sys

import config
from utils import get_connection, setup_logger


SCHEMA = """
CREATE TABLE IF NOT EXISTS estaciones (
    id                  TEXT PRIMARY KEY,
    nombre              TEXT,
    lng                 REAL,
    lat                 REAL,
    altitud             REAL,
    corriente           TEXT,
    zona                TEXT,
    subzona             TEXT,
    cenpoblado          TEXT,
    municipio           TEXT,
    depart              TEXT,
    ctg                 TEXT,
    cotacero            REAL,
    areaaferete         REAL,
    maxnivel            REAL,
    maxcaudal           REAL,
    uroja               REAL,
    unaranja            REAL,
    uamarilla           REAL,
    ubajos              REAL,
    umaxhis             REAL,
    subred              TEXT,

    -- Variables disponibles según el CSV
    NIVEL               INTEGER DEFAULT 0,
    CAUDAL              INTEGER DEFAULT 0,

    -- Estado (lo mantiene update_data.py)
    estado               TEXT DEFAULT 'DESCONOCIDO',
    ultima_actualizacion TEXT,
    ultima_observacion   TEXT
);

CREATE TABLE IF NOT EXISTS mediciones (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    id_estacion TEXT NOT NULL,
    variable    TEXT NOT NULL,    -- Hobs | Hsen | Qobs | Qsen
    timestamp   TEXT NOT NULL,
    valor       REAL NOT NULL,
    UNIQUE(id_estacion, variable, timestamp),
    FOREIGN KEY (id_estacion) REFERENCES estaciones(id)
);

CREATE INDEX IF NOT EXISTS idx_mediciones_est_var_ts
    ON mediciones(id_estacion, variable, timestamp);
CREATE INDEX IF NOT EXISTS idx_mediciones_ts ON mediciones(timestamp);

CREATE TABLE IF NOT EXISTS log_actualizaciones (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha_ejecucion    TEXT NOT NULL,
    id_estacion        TEXT NOT NULL,
    variable           TEXT NOT NULL,    -- Hobs | Hsen | Qobs | Qsen
    status             TEXT NOT NULL,    -- success | error | no_data | not_found
    registros_nuevos   INTEGER DEFAULT 0,
    mensaje            TEXT
);

CREATE INDEX IF NOT EXISTS idx_log_fecha ON log_actualizaciones(fecha_ejecucion);
"""


NUMERIC_COLS = {
    "lng", "lat", "altitud", "cotacero", "areaaferete",
    "maxnivel", "maxcaudal",
    "uroja", "unaranja", "uamarilla", "ubajos", "umaxhis",
}
BOOL_COLS = {"NIVEL", "CAUDAL"}


def _to_int_bool(v) -> int:
    return 1 if str(v).strip().upper() == "TRUE" else 0


def _cast(col, v):
    if col in BOOL_COLS:
        return _to_int_bool(v)
    if col in NUMERIC_COLS:
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    if v == "":
        return None
    return v


def main() -> int:
    log = setup_logger()

    if not config.STATIONS_CSV.exists():
        log.error("No encuentro el CSV maestro en %s", config.STATIONS_CSV)
        log.error("Corre primero: python build_stations_csv.py")
        return 1

    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
        log.info("Esquema creado / verificado en %s", config.DB_PATH)

        # Leer CSV con detección tolerante de encoding
        filas = None
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                with open(config.STATIONS_CSV, newline="", encoding=enc) as f:
                    reader = csv.DictReader(f)
                    filas = list(reader)
                log.info("CSV leído con encoding=%s (%d filas)", enc, len(filas))
                break
            except UnicodeDecodeError:
                continue
        if filas is None:
            log.error("No pude decodificar el CSV.")
            return 1

        cols = config.STATIC_COLUMNS + ["NIVEL", "CAUDAL"]
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(cols)
        # UPSERT: si la estación ya existe, refresca metadatos sin perder
        # estado / mediciones acumuladas.
        sql = (
            f"INSERT INTO estaciones ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET "
            + ", ".join(f"{c}=excluded.{c}" for c in cols if c != "id")
        )

        n_filas = 0
        for row in filas:
            try:
                values = [_cast(c, row.get(c)) for c in cols]
                conn.execute(sql, values)
                n_filas += 1
            except Exception as e:
                log.warning("Fila descartada (%s): %s", row.get("id"), e)

        conn.commit()
        log.info("Estaciones cargadas/actualizadas: %d", n_filas)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
