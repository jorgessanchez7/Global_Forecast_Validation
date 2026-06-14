"""
update_data.py
--------------
Actualización diaria de la BD FEWS IDEAM. Pensado para correr una vez al
día por cron / Task Scheduler.

Para cada estación que el CSV maestro marcó con NIVEL=TRUE o CAUDAL=TRUE:
  - Descarga la serie correspondiente (jsonH y/o jsonQ).
  - Inserta los registros nuevos (UNIQUE + INSERT OR IGNORE → sin duplicar).
  - Si la API responde 404 o error transitorio, lo registra en el log y
    sigue. NO actualiza los flags NIVEL/CAUDAL — esos solo los cambia
    `build_stations_csv.py` (o vuelve a correrlo cuando quieras refrescar
    el catálogo).
  - Recalcula `estado` (ACTIVA / INACTIVA) basado en el dato más reciente
    en `mediciones`. Las INACTIVAS se siguen consultando.

A diferencia de SATMA, la API IDEAM no acepta un parámetro de cantidad de
días: simplemente devuelve los ~30 días que retiene. No hay flag
--initial; cada corrida descarga todo lo que la API tenga y el INSERT OR
IGNORE acumula histórico día a día.

Uso:
    python update_data.py                          # todas las estaciones
    python update_data.py --estacion 0011027030    # solo una
    python update_data.py --max 20                 # primeras 20 (pruebas)
"""
import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

import config
from utils import (
    get_connection,
    http_get_json,
    parse_series,
    setup_logger,
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--estacion", type=str, default=None,
                   help="Actualizar solo esta estación (id como string).")
    p.add_argument("--max", type=int, default=None,
                   help="Procesar a lo sumo N estaciones (pruebas).")
    return p.parse_args()


def get_stations(conn, est_id):
    if est_id is not None:
        rows = conn.execute(
            "SELECT id, NIVEL, CAUDAL FROM estaciones WHERE id = ?", (est_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, NIVEL, CAUDAL FROM estaciones ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def insert_records(conn, est_id, variable, records) -> int:
    if not records:
        return 0
    before = conn.execute(
        "SELECT COUNT(*) FROM mediciones WHERE id_estacion=? AND variable=?",
        (est_id, variable),
    ).fetchone()[0]
    conn.executemany(
        "INSERT OR IGNORE INTO mediciones (id_estacion, variable, timestamp, valor) "
        "VALUES (?, ?, ?, ?)",
        [(est_id, variable, r["timestamp"], r["valor"]) for r in records],
    )
    after = conn.execute(
        "SELECT COUNT(*) FROM mediciones WHERE id_estacion=? AND variable=?",
        (est_id, variable),
    ).fetchone()[0]
    return after - before


def log_call(conn, est_id, variable, status, nuevos, mensaje, now_iso):
    conn.execute(
        "INSERT INTO log_actualizaciones "
        "(fecha_ejecucion, id_estacion, variable, status, registros_nuevos, mensaje) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (now_iso, est_id, variable, status, nuevos, mensaje),
    )


def process_variable(conn, est_id, var_key, now_iso, log) -> int:
    """
    Descarga y guarda la serie (H o Q). Devuelve registros nuevos totales.
    """
    url = config.SERIES_URL[var_key].format(id=est_id)
    sub_keys = config.SERIES_VARIABLES[var_key]  # ('Hobs','Hsen') o ('Qobs','Qsen')

    try:
        status, data = http_get_json(url, allow_404=True)
    except Exception as e:
        log.error("  %s ERROR: %s", var_key, e)
        for sub in sub_keys:
            log_call(conn, est_id, sub, "error", 0, str(e), now_iso)
        return 0

    if status == 404:
        for sub in sub_keys:
            log_call(conn, est_id, sub, "not_found", 0, "404", now_iso)
        log.info("  %s -> 404 (no disponible hoy)", var_key)
        return 0

    if status != 200 or data is None:
        for sub in sub_keys:
            log_call(conn, est_id, sub, "error", 0, f"status={status}", now_iso)
        return 0

    parsed = parse_series(data, sub_keys)
    total = 0
    for sub in sub_keys:
        records = parsed.get(sub, [])
        nuevos = insert_records(conn, est_id, sub, records)
        total += nuevos
        status_str = "success" if records else "no_data"
        log_call(conn, est_id, sub, status_str, nuevos,
                 f"{len(records)} recibidos, {nuevos} nuevos", now_iso)
        log.info("    %-6s %d recibidos, %d nuevos", sub, len(records), nuevos)
    return total


def recompute_state(conn, est_id, now_iso):
    threshold_ts = (
        datetime.now(timezone.utc) - timedelta(days=config.INACTIVITY_THRESHOLD_DAYS)
    ).strftime("%Y-%m-%dT%H:%M:%S")

    row = conn.execute(
        "SELECT MAX(timestamp) AS ult FROM mediciones WHERE id_estacion=?",
        (est_id,),
    ).fetchone()
    ult = row["ult"] if row else None

    if ult is None:
        estado = "INACTIVA"
    else:
        estado = "ACTIVA" if ult >= threshold_ts else "INACTIVA"

    conn.execute(
        "UPDATE estaciones SET estado=?, ultima_actualizacion=?, ultima_observacion=? "
        "WHERE id=?",
        (estado, now_iso, ult, est_id),
    )
    return estado, ult


def main() -> int:
    args = parse_args()
    log = setup_logger()
    conn = get_connection()
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    estaciones = get_stations(conn, args.estacion)
    if not estaciones:
        log.error("No hay estaciones en la BD. Corre primero build_stations_csv.py y init_db.py")
        return 1
    if args.max:
        estaciones = estaciones[:args.max]

    # Solo las que el CSV marcó como con datos
    elegibles = [e for e in estaciones if e["NIVEL"] or e["CAUDAL"]]
    log.info("Estaciones en BD: %d. Con NIVEL o CAUDAL=TRUE: %d",
             len(estaciones), len(elegibles))

    total_llamadas = total_nuevos = total_errores = activas = 0

    for i, est in enumerate(elegibles, 1):
        est_id = est["id"]
        if i % 50 == 0 or i == 1:
            log.info("Progreso: %d / %d", i, len(elegibles))
        log.info("[%s] NIVEL=%s CAUDAL=%s",
                 est_id, bool(est["NIVEL"]), bool(est["CAUDAL"]))

        if est["NIVEL"]:
            total_nuevos += process_variable(conn, est_id, "H", now_iso, log)
            total_llamadas += 1
            time.sleep(config.HTTP_SLEEP_BETWEEN_CALLS)

        if est["CAUDAL"]:
            total_nuevos += process_variable(conn, est_id, "Q", now_iso, log)
            total_llamadas += 1
            time.sleep(config.HTTP_SLEEP_BETWEEN_CALLS)

        estado, ult = recompute_state(conn, est_id, now_iso)
        if estado == "ACTIVA":
            activas += 1
        log.info("  -> estado=%s (última obs: %s)", estado, ult)

        conn.commit()

    log.info("Fin. Llamadas=%d, nuevos=%d, activas=%d/%d",
             total_llamadas, total_nuevos, activas, len(elegibles))
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
