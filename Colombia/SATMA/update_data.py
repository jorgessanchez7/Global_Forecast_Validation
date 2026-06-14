"""
update_data.py
--------------
Script de actualización. Está pensado para correr una vez al día (cron, Task
Scheduler, GitHub Actions, etc.) pero también sirve para la carga histórica
inicial con --initial (pide 180 días en vez de 2).

Para cada estación:
  - Recorre solamente las variables marcadas como disponibles (TRUE) en el CSV.
  - Llama a la API y guarda los registros nuevos (INSERT OR IGNORE evita duplicados).
  - Aunque la estación esté INACTIVA, sigue siendo consultada (los datos pueden
    volver a aparecer).
  - Tras procesar todas sus variables, recalcula el estado:
        * ACTIVA   si tiene al menos un dato en los últimos INACTIVITY_THRESHOLD_DAYS.
        * INACTIVA si no.
  - Registra cada llamada en log_actualizaciones.

Uso:
    python update_data.py            # actualización diaria (dias=2)
    python update_data.py --initial  # carga histórica completa (dias=180)
    python update_data.py --estacion 16  # solo una estación
"""
import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

import config
from utils import fetch_registros, get_connection, setup_logger


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Actualiza datos SATMA en la BD local.")
    p.add_argument(
        "--initial",
        action="store_true",
        help=f"Carga histórica: pide {config.DAYS_INITIAL_LOAD} días en vez de "
             f"{config.DAYS_DAILY_UPDATE}.",
    )
    p.add_argument(
        "--estacion",
        type=int,
        default=None,
        help="Actualizar solo la estación con este idEstacion.",
    )
    p.add_argument(
        "--variable",
        type=str,
        default=None,
        help="Actualizar solo esta variable (debe estar en config.VARIABLES).",
    )
    return p.parse_args()


def get_stations(conn, id_estacion: int | None) -> list[dict]:
    if id_estacion is not None:
        rows = conn.execute(
            "SELECT * FROM estaciones WHERE idEstacion = ?", (id_estacion,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM estaciones ORDER BY idEstacion"
        ).fetchall()
    return [dict(r) for r in rows]


def insert_records(conn, id_est: int, variable: str, records: list[dict]) -> int:
    """Inserta solo los registros nuevos. Devuelve cuántos eran nuevos."""
    if not records:
        return 0
    before = conn.execute(
        "SELECT COUNT(*) FROM mediciones WHERE idEstacion=? AND variable=?",
        (id_est, variable),
    ).fetchone()[0]
    conn.executemany(
        "INSERT OR IGNORE INTO mediciones (idEstacion, variable, timestamp, valor) "
        "VALUES (?, ?, ?, ?)",
        [(id_est, variable, r["timestamp"], r["valor"]) for r in records],
    )
    after = conn.execute(
        "SELECT COUNT(*) FROM mediciones WHERE idEstacion=? AND variable=?",
        (id_est, variable),
    ).fetchone()[0]
    return after - before


def log_call(conn, id_est, variable, status, nuevos, mensaje, now_iso):
    conn.execute(
        "INSERT INTO log_actualizaciones "
        "(fecha_ejecucion, idEstacion, variable, status, registros_nuevos, mensaje) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (now_iso, id_est, variable, status, nuevos, mensaje),
    )


def recompute_state(conn, id_est: int, now_iso: str) -> tuple[str, str | None]:
    """
    Calcula y guarda el estado de la estación.
    Devuelve (estado, ultima_observacion_iso).
    """
    threshold_ts = (
        datetime.now(timezone.utc) - timedelta(days=config.INACTIVITY_THRESHOLD_DAYS)
    ).isoformat()

    row = conn.execute(
        "SELECT MAX(timestamp) AS ult FROM mediciones WHERE idEstacion=?",
        (id_est,),
    ).fetchone()
    ult = row["ult"] if row else None

    if ult is None:
        estado = "INACTIVA"
    else:
        # comparación lexicográfica de timestamps ISO 8601 es válida
        estado = "ACTIVA" if ult >= threshold_ts else "INACTIVA"

    conn.execute(
        "UPDATE estaciones "
        "   SET estado=?, ultima_actualizacion=?, ultima_observacion=? "
        " WHERE idEstacion=?",
        (estado, now_iso, ult, id_est),
    )
    return estado, ult


def main() -> int:
    args = parse_args()
    log = setup_logger()
    conn = get_connection()

    dias = config.DAYS_INITIAL_LOAD if args.initial else config.DAYS_DAILY_UPDATE
    now_iso = datetime.now(timezone.utc).isoformat()
    log.info(
        "Inicio actualización (modo=%s, dias=%d, estacion=%s)",
        "initial" if args.initial else "diaria",
        dias,
        args.estacion or "todas",
    )

    estaciones = get_stations(conn, args.estacion)
    if not estaciones:
        log.error("No hay estaciones en la BD. ¿Corriste init_db.py?")
        return 1

    total_nuevos = 0
    total_llamadas = 0
    total_errores = 0

    for est in estaciones:
        id_est = est["idEstacion"]
        cod = est["codEstacion"]

        # variables soportadas por esta estación (según CSV)
        vars_estacion = [v for v in config.VARIABLES if est.get(v) == 1]
        if args.variable:
            vars_estacion = [v for v in vars_estacion if v == args.variable]

        if not vars_estacion:
            log.info("[%s id=%s] no tiene variables habilitadas, salto.", cod, id_est)
            continue

        log.info("[%s id=%s] %d variables a consultar", cod, id_est, len(vars_estacion))

        for var in vars_estacion:
            total_llamadas += 1
            try:
                records = fetch_registros(id_est, var, dias)
                nuevos = insert_records(conn, id_est, var, records)
                total_nuevos += nuevos
                status = "success" if records else "no_data"
                msg = f"{len(records)} registros recibidos, {nuevos} nuevos"
                log_call(conn, id_est, var, status, nuevos, msg, now_iso)
                log.info("  %-20s %s", var, msg)
            except Exception as e:
                total_errores += 1
                log_call(conn, id_est, var, "error", 0, str(e), now_iso)
                log.error("  %-20s ERROR: %s", var, e)

            time.sleep(config.API_SLEEP_BETWEEN_CALLS)

        # Recalcular estado de la estación tras procesar todas sus variables
        estado, ult = recompute_state(conn, id_est, now_iso)
        log.info("  -> estado=%s (última observación: %s)", estado, ult)

        conn.commit()  # commit por estación, robusto ante caídas

    log.info(
        "Fin. Llamadas=%d, errores=%d, registros nuevos=%d",
        total_llamadas, total_errores, total_nuevos,
    )
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
