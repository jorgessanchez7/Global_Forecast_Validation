"""
ingest_historical.py
--------------------
Carga histórica de los archivos Delft-FEWS a la BD FEWS IDEAM.

Lee cada xlsx, convierte cada celda no-NaN a una fila
(id_estacion, variable, timestamp, valor) y la inserta en `mediciones`.
La columna `variable` queda en {'Hobs', 'Hsen', 'Qobs', 'Qsen'} según
el archivo de origen.

Solo se corre una vez tras `init_db.py`. Es idempotente: el UNIQUE +
INSERT OR IGNORE evita duplicar si se vuelve a ejecutar.

Al final, recalcula el estado (ACTIVA/INACTIVA) de cada estación que
recibió datos, usando el mismo criterio que update_data.py.

Uso:
    python ingest_historical.py
"""
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

import config
from utils import get_connection, setup_logger
from update_data import recompute_state


# Mapeo nombre de archivo -> nombre de variable en la tabla mediciones.
# Los archivos se buscan en config.BASE_DIR.
ARCHIVOS = [
    ("Niveles_convencionales.xlsx", "Hobs"),
    ("Nivels_automaticas.xlsx",     "Hsen"),
    ("Caudales_convencionales.xlsx",         "Qobs"),
    ("Caudales_automaticas.xlsx",   "Qsen"),
]


# ---------------------------------------------------------------------------
# Match de IDs entre xlsx y BD
# ---------------------------------------------------------------------------
def id_from_cell(v):
    """Normaliza un id de celda del xlsx. Excel suele cargarlos como float
    (16017010.0), pero también pueden venir como int o string ('H0011')."""
    if pd.isna(v):
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s


def build_id_map(conn):
    """
    Construye {clave_de_busqueda: id_canonico_en_BD} para hacer match.

    Los xlsx vienen con IDs sin zero-padding ('16017010', 'H0011'), pero
    en la BD pueden estar con padding distinto ('0016017010'). El mapa
    permite que '16017010' encuentre a '0016017010'.
    """
    rows = conn.execute("SELECT id FROM estaciones").fetchall()
    out = {}
    for r in rows:
        id_bd = r["id"]
        out[id_bd] = id_bd                          # match exacto
        stripped = id_bd.lstrip("0")
        if stripped and stripped != id_bd:
            out.setdefault(stripped, id_bd)         # sin ceros a la izquierda
    return out


# ---------------------------------------------------------------------------
# Ingesta de un archivo
# ---------------------------------------------------------------------------
def ingest_file(conn, log, path, variable, id_map):
    if not path.exists():
        log.warning("No existe %s -- saltando", path.name)
        return set()

    log.info("Leyendo %s ...", path.name)
    df = pd.read_excel(path, header=None)
    n_rows, n_cols = df.shape
    log.info("  shape = %d x %d", n_rows, n_cols)

    # Timestamps de la columna 0 desde fila 4 (las primeras 4 son header Delft-FEWS)
    ts_col = pd.to_datetime(df.iloc[4:, 0], errors='coerce').reset_index(drop=True)

    estaciones_tocadas = set()
    n_no_match = 0
    n_insertados_estimado = 0

    for c in range(1, n_cols):
        raw_id = id_from_cell(df.iloc[3, c])
        if not raw_id:
            continue
        id_bd = id_map.get(raw_id)
        if id_bd is None:
            n_no_match += 1
            log.debug("  id %r no encontrada en BD", raw_id)
            continue

        valores = pd.to_numeric(df.iloc[4:, c], errors='coerce').reset_index(drop=True)
        mask = valores.notna() & ts_col.notna()
        if not mask.any():
            continue

        registros = [
            (id_bd, variable,
             ts_col.iloc[i].strftime("%Y-%m-%dT%H:%M:%S"),
             float(valores.iloc[i]))
            for i in mask[mask].index
        ]

        if registros:
            conn.executemany(
                "INSERT OR IGNORE INTO mediciones "
                "(id_estacion, variable, timestamp, valor) VALUES (?, ?, ?, ?)",
                registros,
            )
            n_insertados_estimado += len(registros)
            estaciones_tocadas.add(id_bd)

    conn.commit()
    log.info("  %s: %d registros procesados, %d estaciones, %d IDs sin match",
             variable, n_insertados_estimado, len(estaciones_tocadas), n_no_match)
    return estaciones_tocadas


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    log = setup_logger()
    conn = get_connection()
    log.info("=== Ingesta histórica FEWS IDEAM ===")

    id_map = build_id_map(conn)
    log.info("Mapa de IDs: %d entradas (exactos + sin zero-padding)", len(id_map))

    estaciones_total = set()
    for filename, variable in ARCHIVOS:
        path = config.BASE_DIR / filename
        ests = ingest_file(conn, log, path, variable, id_map)
        estaciones_total.update(ests)

    # Recalcular estado para todas las estaciones que recibieron datos
    log.info("Recalculando estado de %d estaciones...", len(estaciones_total))
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for est_id in estaciones_total:
        recompute_state(conn, est_id, now_iso)
    conn.commit()

    # Resumen
    n_med = conn.execute("SELECT COUNT(*) FROM mediciones").fetchone()[0]
    n_activas = conn.execute("SELECT COUNT(*) FROM estaciones WHERE estado='ACTIVA'").fetchone()[0]
    n_inactivas = conn.execute("SELECT COUNT(*) FROM estaciones WHERE estado='INACTIVA'").fetchone()[0]
    n_descon = conn.execute("SELECT COUNT(*) FROM estaciones WHERE estado='DESCONOCIDO'").fetchone()[0]
    log.info("=" * 50)
    log.info("Total mediciones en BD:  %d", n_med)
    log.info("Estaciones ACTIVA:       %d", n_activas)
    log.info("Estaciones INACTIVA:     %d", n_inactivas)
    log.info("Estaciones DESCONOCIDO:  %d", n_descon)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
