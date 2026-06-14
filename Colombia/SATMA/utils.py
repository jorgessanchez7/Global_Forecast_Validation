"""
Utilidades compartidas: cliente HTTP, manejo de logs y conexión a SQLite.
"""
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import requests

import config


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logger(name: str = "satma") -> logging.Logger:
    """Configura un logger que escribe a archivo y a consola."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # ya configurado

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_file = config.LOGS_DIR / f"satma_{datetime.now():%Y%m%d}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ---------------------------------------------------------------------------
# Conexión SQLite
# ---------------------------------------------------------------------------
def get_connection(db_path: Path = None) -> sqlite3.Connection:
    """Devuelve una conexión SQLite con configuración recomendada."""
    db_path = db_path or config.DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Mejoras de rendimiento y concurrencia
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------------------------------------------------------------------------
# Cliente API
# ---------------------------------------------------------------------------
def fetch_registros(id_estacion: int, variable: str, dias: int) -> list[dict]:
    """
    Llama a la API de SATMA y devuelve la lista de registros.

    Devuelve [] si la API responde vacío o si no hay datos.
    Lanza excepción si todos los reintentos fallan.

    Se espera que la API devuelva un JSON, idealmente una lista de objetos
    del estilo [{"fecha": "...", "valor": ...}, ...]. Esta función intenta
    normalizar varios formatos comunes.
    """
    log = logging.getLogger("satma")
    params = {"dias": dias, "idEstacion": id_estacion, "variable": variable}

    last_err = None
    for intento in range(1, config.API_MAX_RETRIES + 1):
        try:
            r = requests.get(
                config.API_BASE_URL,
                params=params,
                timeout=config.API_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json()
            return _normalize_records(data)
        except (requests.RequestException, ValueError) as e:
            last_err = e
            log.warning(
                "API fallo (intento %d/%d) idEstacion=%s variable=%s: %s",
                intento, config.API_MAX_RETRIES, id_estacion, variable, e,
            )
            time.sleep(config.API_RETRY_BACKOFF * intento)

    raise RuntimeError(f"API falló tras {config.API_MAX_RETRIES} intentos: {last_err}")


def _normalize_records(data) -> list[dict]:
    """
    Convierte la respuesta de la API SATMA en una lista uniforme de
    diccionarios con las claves 'timestamp' (str) y 'valor' (float).

    Formato real de la API SATMA:
        [
          {
            "id": 84,
            "variable": "riverFlow",
            "legend": "Caudal del Rio",
            "unit": "m³/s",
            "threshold": [...],          # puede estar vacío o traer warning/critical
            "data": [
              {"time": "2025-11-17T00:01:00", "value": 1.9825},
              ...
            ]
          }
        ]

    Se toleran además un par de variantes por robustez (lista plana,
    dict con clave 'data'), por si la API cambia o algún endpoint
    devuelve algo distinto.
    """
    if not data:
        return []

    # Caso principal: lista de envoltorios
    if isinstance(data, list):
        out = []
        for item in data:
            if not isinstance(item, dict):
                continue
            inner = item.get("data")
            if isinstance(inner, list):
                # envoltorio estándar SATMA
                out.extend(_extract_points(inner))
            else:
                # tal vez es una lista plana de {time, value}
                pt = _extract_point(item)
                if pt:
                    out.append(pt)
        return out

    # Caso alternativo: dict con clave 'data'
    if isinstance(data, dict):
        for k in ("data", "registros", "records", "result", "results"):
            v = data.get(k)
            if isinstance(v, list):
                return _extract_points(v)

    return []


def _extract_points(records: list) -> list[dict]:
    out = []
    for rec in records:
        pt = _extract_point(rec)
        if pt is not None:
            out.append(pt)
    return out


def _extract_point(rec) -> dict | None:
    """Extrae un único punto {timestamp, valor} de un dict de registro."""
    if not isinstance(rec, dict):
        return None
    ts = (
        rec.get("time")
        or rec.get("timestamp")
        or rec.get("fecha")
        or rec.get("date")
        or rec.get("datetime")
    )
    val = rec.get("value")
    if val is None:
        val = rec.get("valor")
    if ts is None or val is None:
        return None
    try:
        val = float(val)
    except (TypeError, ValueError):
        return None
    return {"timestamp": str(ts), "valor": val}


def extract_metadata(data) -> dict:
    """
    Devuelve metadatos del envoltorio (legend, unit, threshold) si están.
    Útil para guardarlos junto a la estación.
    """
    if isinstance(data, list) and data and isinstance(data[0], dict):
        item = data[0]
        return {
            "legend": item.get("legend"),
            "unit": item.get("unit"),
            "threshold": item.get("threshold"),
        }
    return {}
