"""
export_csv.py
-------------
Exporta el contenido de la base de datos a CSV. Útil para compartir,
analizar en Excel, o como respaldo legible.

Genera en config.EXPORT_DIR:
  - estaciones.csv         (metadatos + estado actual)
  - mediciones/{idEstacion}_{variable}.csv  (una serie por archivo)

Uso:
    python export_csv.py                 # exporta todo
    python export_csv.py --estacion 16   # exporta solo una estación
"""
import argparse
import csv
import sys

import config
from utils import get_connection, setup_logger


def export_estaciones(conn, out_dir):
    rows = conn.execute("SELECT * FROM estaciones ORDER BY idEstacion").fetchall()
    if not rows:
        return 0
    path = out_dir / "estaciones.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(rows[0].keys())
        for r in rows:
            writer.writerow(list(r))
    return len(rows)


def export_series(conn, out_dir, id_est=None):
    sub = out_dir / "mediciones"
    sub.mkdir(parents=True, exist_ok=True)

    sql = (
        "SELECT idEstacion, variable, timestamp, valor "
        "FROM mediciones "
    )
    params = ()
    if id_est is not None:
        sql += "WHERE idEstacion = ? "
        params = (id_est,)
    sql += "ORDER BY idEstacion, variable, timestamp"

    cur = conn.execute(sql, params)

    current_key = None
    f = None
    writer = None
    total = 0

    for row in cur:
        key = (row["idEstacion"], row["variable"])
        if key != current_key:
            if f:
                f.close()
            current_key = key
            fname = sub / f"{row['idEstacion']}_{row['variable']}.csv"
            f = open(fname, "w", newline="", encoding="utf-8")
            writer = csv.writer(f)
            var = row["variable"]
            unidad = config.VARIABLE_UNITS.get(var, "")
            header_valor = f"{var} ({unidad})" if unidad else var
            writer.writerow(["datetime", header_valor])
        writer.writerow([row["timestamp"], row["valor"]])
        total += 1

    if f:
        f.close()
    return total


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--estacion", type=int, default=None)
    args = parser.parse_args()

    log = setup_logger()
    conn = get_connection()

    n_est = export_estaciones(conn, config.EXPORT_DIR)
    log.info("Exportadas %d estaciones a %s", n_est, config.EXPORT_DIR / "estaciones.csv")

    n_med = export_series(conn, config.EXPORT_DIR, args.estacion)
    log.info("Exportadas %d mediciones a %s/mediciones/", n_med, config.EXPORT_DIR)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
