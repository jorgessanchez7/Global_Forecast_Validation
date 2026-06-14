"""
build_stations_csv.py
---------------------
Construye el CSV maestro de estaciones FEWS IDEAM. Este es el PRIMER PASO
del flujo (solo se corre una vez, o cuando quieras refrescar el catálogo
porque sospechas que aparecieron estaciones nuevas).

Hace lo siguiente:
  1. Descarga los 5 endpoints GeoJSON del IDEAM.
  2. Une todas las estaciones por `id` (deduplicación). Cuando una columna
     común tiene valores distintos en dos fuentes, gana la de mayor
     prioridad: H > Hsim > Q > Qsim > Calidad.
  3. Para cada estación, prueba GET /visorfews/data/series/jsonH/{id}.json
     y /jsonQ/{id}.json para determinar si la estación tiene serie de
     NIVEL y/o CAUDAL disponible.
  4. Escribe FEWS_estaciones.csv con todas las columnas estáticas + flags
     `en_H/en_Hsim/en_Q/en_Qsim/en_Calidad` + `NIVEL/CAUDAL` como
     TRUE/FALSE.

Análogo al CSV de SATMA: una vez generado, lo puedes inspeccionar, editar
o filtrar manualmente. Después `init_db.py` lo lee y construye la BD.

Uso:
    python build_stations_csv.py
    python build_stations_csv.py --skip-discovery   # no probar NIVEL/CAUDAL
    python build_stations_csv.py --max 50           # solo primeras 50 (pruebas)
"""
import argparse
import csv
import sys
import time

import config
from utils import (
    http_get_json,
    parse_station_features,
    setup_logger,
)


# Columnas que escribimos en el CSV maestro
CSV_COLUMNS = config.STATIC_COLUMNS + ["NIVEL", "CAUDAL"]

NUMERIC_COLS = {
    "lng", "lat", "altitud", "cotacero", "areaaferete",
    "maxnivel", "maxcaudal",
    "uroja", "unaranja", "uamarilla", "ubajos", "umaxhis",
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--skip-discovery", action="store_true",
                   help="Salta la verificación NIVEL/CAUDAL (queda como FALSE en el CSV).")
    p.add_argument("--max", type=int, default=None,
                   help="Procesar a lo sumo N estaciones (pruebas).")
    p.add_argument("--output", type=str, default=None,
                   help="Ruta de salida del CSV (por defecto config.STATIONS_CSV).")
    return p.parse_args()


def merge_station(master: dict, props: dict, fuente: str, log) -> None:
    """
    Mergea propiedades de un endpoint sobre el dict maestro de la estación.
    Marca en qué endpoint apareció. Para columnas con valor en conflicto,
    deja el primer valor encontrado (orden = prioridad en STATION_ENDPOINTS).
    """
    master[f"en_{fuente}"] = 1
    for col in config.STATIC_COLUMNS:
        if col not in props:
            continue
        new_val = props[col]
        if new_val is None or new_val == "":
            continue
        cur = master.get(col)
        if cur is None or cur == "":
            master[col] = new_val
        elif cur != new_val:
            log.debug(
                "Conflicto %s.%s: existente=%r (mayor prioridad) vs %s=%r",
                master.get("id"), col, cur, fuente, new_val,
            )


def download_all_stations(log) -> dict[str, dict]:
    """Descarga los 5 endpoints y devuelve {id: merged_dict}."""
    master: dict[str, dict] = {}

    for fuente, url in config.STATION_ENDPOINTS:
        try:
            log.info("Descargando %s...", fuente)
            _, data = http_get_json(url, allow_404=False)
            feats = parse_station_features(data)
            log.info("  %s: %d estaciones", fuente, len(feats))
        except Exception as e:
            log.error("  %s: ERROR -> %s", fuente, e)
            continue

        for props in feats:
            est_id = props.get("id")
            if not est_id:
                continue
            est_id = str(est_id).strip()
            if not est_id:
                continue
            if est_id not in master:
                master[est_id] = {"id": est_id}
                for f, _ in config.STATION_ENDPOINTS:
                    master[est_id][f"en_{f}"] = 0
                master[est_id]["NIVEL"] = 0
                master[est_id]["CAUDAL"] = 0
            merge_station(master[est_id], props, fuente, log)

    return master


def discover_variables(master: dict, log) -> None:
    """
    Para cada estación, prueba /jsonH/ y /jsonQ/. Si responde 200 con datos
    -> marca el flag correspondiente en 1.
    """
    total = len(master)
    for i, (est_id, data) in enumerate(master.items(), 1):
        if i % 50 == 0 or i == 1:
            log.info("Descubrimiento NIVEL/CAUDAL: %d / %d", i, total)

        for var_key, flag in (("H", "NIVEL"), ("Q", "CAUDAL")):
            url = config.SERIES_URL[var_key].format(id=est_id)
            try:
                status, payload = http_get_json(url, allow_404=True)
                if status == 200 and payload:
                    data[flag] = 1
                else:
                    data[flag] = 0
            except Exception as e:
                log.warning("[%s] %s descubrimiento falló: %s -> queda 0",
                            est_id, var_key, e)
                data[flag] = 0
            time.sleep(config.HTTP_SLEEP_BETWEEN_CALLS)


def write_csv(master: dict, output_path, log) -> int:
    """Escribe el CSV ordenado por id. Devuelve cuántas filas escribió."""
    rows = sorted(master.values(), key=lambda d: d.get("id", ""))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        for row in rows:
            out = []
            for col in CSV_COLUMNS:
                v = row.get(col)
                if col in ("NIVEL", "CAUDAL"):
                    out.append("TRUE" if v else "FALSE")
                elif col in NUMERIC_COLS and v is not None:
                    try:
                        out.append(float(v))
                    except (TypeError, ValueError):
                        out.append("")
                elif v is None:
                    out.append("")
                else:
                    out.append(v)
            writer.writerow(out)

    log.info("CSV escrito: %s (%d filas)", output_path, len(rows))
    return len(rows)


def main() -> int:
    args = parse_args()
    log = setup_logger()
    output_path = args.output or config.STATIONS_CSV

    # 1. Descargar y unificar
    master = download_all_stations(log)
    log.info("Estaciones únicas: %d", len(master))
    for f, _ in config.STATION_ENDPOINTS:
        n = sum(1 for m in master.values() if m.get(f"en_{f}"))
        log.info("  presentes en %s: %d", f, n)

    if not master:
        log.error("No se obtuvieron estaciones. Abortando.")
        return 1

    if args.max:
        keep_ids = sorted(master.keys())[:args.max]
        master = {k: master[k] for k in keep_ids}
        log.info("Modo prueba: limitado a %d estaciones", len(master))

    # 2. Descubrir NIVEL/CAUDAL (a menos que se haya pedido saltar)
    if not args.skip_discovery:
        log.info("Iniciando descubrimiento de NIVEL/CAUDAL (~%d llamadas)...",
                 2 * len(master))
        discover_variables(master, log)
        n_nivel = sum(1 for m in master.values() if m.get("NIVEL"))
        n_caudal = sum(1 for m in master.values() if m.get("CAUDAL"))
        log.info("Con NIVEL=TRUE:  %d", n_nivel)
        log.info("Con CAUDAL=TRUE: %d", n_caudal)
    else:
        log.info("--skip-discovery: NIVEL/CAUDAL quedan en FALSE.")

    # 3. Escribir CSV
    write_csv(master, output_path, log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
