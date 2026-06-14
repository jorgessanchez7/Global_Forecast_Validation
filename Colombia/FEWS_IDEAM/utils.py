"""
Utilidades compartidas: cliente HTTP con verify=False, parsers de los
JSON del IDEAM, logger y conexión SQLite.
"""
import logging
import sqlite3
import time
import unicodedata
import warnings
from datetime import datetime
from pathlib import Path

import requests
import urllib3

import config

# Silenciar advertencias de SSL ya que el IDEAM usa cert autofirmado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logger(name: str = "fews") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_file = config.LOGS_DIR / f"fews_{datetime.now():%Y%m%d}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
def get_connection(db_path: Path = None) -> sqlite3.Connection:
    db_path = db_path or config.DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------------------------------------------------------------------------
# Cliente HTTP
# ---------------------------------------------------------------------------
_session = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": config.HTTP_USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Referer": "https://fews.ideam.gov.co/",
        })
        _session = s
    return _session


def http_get_json(url: str, allow_404: bool = True) -> tuple[int, dict | list | None]:
    """
    Hace GET con reintentos. Devuelve (status_code, payload).
    - Si status==200 y el body es JSON válido: (200, parsed_json)
    - Si status==404 y allow_404: (404, None)  ← no es un error
    - En cualquier otra falla: lanza excepción
    """
    log = logging.getLogger("fews")
    s = _get_session()

    last_err = None
    for intento in range(1, config.HTTP_MAX_RETRIES + 1):
        try:
            r = s.get(
                url,
                timeout=config.HTTP_TIMEOUT,
                verify=config.HTTP_VERIFY_SSL,
            )
            if r.status_code == 404:
                if allow_404:
                    return 404, None
                raise requests.HTTPError(f"404 en {url}")
            if r.status_code == 200:
                # El servidor del IDEAM, cuando una URL de serie no existe,
                # no devuelve 404: redirige al frontend (SPA) y devuelve HTML.
                # Eso lo tratamos como "URL no disponible".
                head = r.text.lstrip()[:50].lower()
                if head.startswith("<!doctype") or head.startswith("<html"):
                    if allow_404:
                        return 404, None
                    raise ValueError(f"Respuesta HTML (no JSON) en {url}")
                try:
                    return 200, r.json()
                except ValueError as e:
                    # No JSON pero tampoco HTML. Puede ser un error real.
                    snippet = r.text[:200]
                    raise ValueError(f"Respuesta no es JSON: {snippet!r}") from e
            # Cualquier otro código (5xx, 503 de upstream TLS, etc.): reintentar
            last_err = requests.HTTPError(
                f"HTTP {r.status_code} en {url}: {r.text[:200]!r}"
            )
        except (requests.RequestException, ValueError) as e:
            last_err = e

        log.debug("HTTP fallo (intento %d/%d) %s: %s",
                  intento, config.HTTP_MAX_RETRIES, url, last_err)
        time.sleep(config.HTTP_RETRY_BACKOFF * intento)

    raise RuntimeError(f"GET falló tras {config.HTTP_MAX_RETRIES} intentos: {last_err}")


# ---------------------------------------------------------------------------
# Parsers FEWS IDEAM
# ---------------------------------------------------------------------------
def _to_ascii(s):
    """
    Normaliza un string a ASCII puro: quita tildes y caracteres no-ASCII.
    Ejemplos:
      'NARIÑO'      -> 'NARINO'
      'Darién'      -> 'Darien'
      'Limnigráfica'-> 'Limnigrafica'
    Cualquier valor no-string se devuelve sin tocar.
    """
    if not isinstance(s, str):
        return s
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def parse_station_features(data) -> list[dict]:
    """
    De un GeoJSON FeatureCollection, devuelve una lista de dicts con las
    propiedades de cada feature. Los valores string se normalizan a ASCII.
    """
    if not isinstance(data, dict):
        return []
    feats = data.get("features", [])
    out = []
    for f in feats:
        if not isinstance(f, dict):
            continue
        props = f.get("properties")
        if isinstance(props, dict):
            cleaned = {k: _to_ascii(v) for k, v in props.items()}
            out.append(cleaned)
    return out


def parse_series(data, sub_keys: tuple[str, str]) -> dict[str, list[dict]]:
    """
    Convierte una respuesta tipo:
        {"Hobs": {"label": "...", "data": [{"Fecha": "...", "Hobs": ...}, ..., {"ultimo": true}]},
         "Hsen": {"label": "...", "data": [...]}}
    en:
        {"Hobs": [{"timestamp": "ISO", "valor": float}, ...],
         "Hsen": [...]}

    Filtra:
      - El centinela {"ultimo": true}
      - Registros con valor null
    """
    result = {k: [] for k in sub_keys}
    if not isinstance(data, dict):
        return result

    for k in sub_keys:
        sub = data.get(k)
        if not isinstance(sub, dict):
            continue
        records = sub.get("data", [])
        if not isinstance(records, list):
            continue
        for rec in records:
            if not isinstance(rec, dict):
                continue
            if rec.get("ultimo"):  # centinela final
                continue
            fecha = rec.get("Fecha")
            val = rec.get(k)
            if fecha is None or val is None:
                continue
            ts = _normalize_timestamp(fecha)
            if ts is None:
                continue
            try:
                val = float(val)
            except (TypeError, ValueError):
                continue
            result[k].append({"timestamp": ts, "valor": val})
    return result


def _normalize_timestamp(fecha) -> str | None:
    """
    Convierte 'YYYY/MM/DD HH:MM' (con posibles espacios extra) a ISO 8601
    estricto 'YYYY-MM-DDTHH:MM:00'. Devuelve None si no se reconoce.
    """
    if not isinstance(fecha, str):
        return None
    s = " ".join(fecha.split())  # colapsa espacios múltiples
    for fmt in ("%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return None
