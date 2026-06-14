from __future__ import annotations

import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_VIRTUAL_DIR = (
    "https://cmr.earthdata.nasa.gov/virtual-directory/collections/"
    "C3233944997-POCLOUD/temporal/{YYYY}/{MM}/{DD}"
)

OUTPUT_ROOT = Path(r"E:\SWOT\Water_Surface_Elevation")
START_DATE = "2023-03-28"
END_DATE = "2026-06-12"
OVERWRITE_EXISTING = False

# Leer token desde variable de entorno
#EARTHDATA_TOKEN = os.environ.get("EARTHDATA_TOKEN", "").strip()
EARTHDATA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Impvcmdlc3NhbmNoZXo3IiwiZXhwIjoxNzgzODk3NDE5LCJpYXQiOjE3Nzg3MTM0MTksImlzcyI6Imh0dHBzOi8vdXJzLmVhcnRoZGF0YS5uYXNhLmdvdiIsImlkZW50aXR5X3Byb3ZpZGVyIjoiZWRsX29wcyIsImFjciI6ImVkbCIsImFzc3VyYW5jZV9sZXZlbCI6M30.xVXDMhD2vYT6bt1ew5xTHKhHvwR0o1-aMSUXA1sXrWOQvsprEY8eEkhOY84BWqEs0M-vHrAyloI0Q1H60HaCeL8x-JG30tZvWmgSrZtW9GYgKilCOk0KcSmdtL48H7z_pXMMJXUhMImTDr2gvqYBQt9AGzU1wnV7F-RD2z8IODKMSY7PULgwoRXs46-3cTJm51sui0I35sHgLk31kwirc2GyZXsVbGpX1I39VAcu7TGjNlxFjPS5W_drwdACZKZpNRbQL0OVb_KM6Hx7mh2aT5D8-5m-5W2Yogw4VF-qW9BFJlQXuf0nOUvNvxinbVTIGNFKQ41mSflS_LOSfmMa2g"

SLEEP_BETWEEN_REQUESTS = 0.2
REQUEST_TIMEOUT = 120

# True = borra archivos Node ya descargados antes de empezar
DELETE_EXISTING_NODE_FILES = True


def build_session(earthdata_token: str) -> requests.Session:
    if not earthdata_token:
        raise ValueError("No se encontró EARTHDATA_TOKEN.")

    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"],
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Authorization": f"Bearer {earthdata_token}",
    })

    return session


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def date_range(start_date: datetime, end_date: datetime):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def get_virtual_directory_url(date_obj: datetime) -> str:
    return BASE_VIRTUAL_DIR.format(
        YYYY=date_obj.strftime("%Y"),
        MM=date_obj.strftime("%m"),
        DD=date_obj.strftime("%d"),
    )

def is_node_name(name: str) -> bool:
    return "Node" in name

def get_download_links_for_day(session: requests.Session, date_obj: datetime) -> list[str]:
    url = get_virtual_directory_url(date_obj)
    print(f"\n[INFO] Revisando {date_obj.strftime('%Y-%m-%d')}")
    print(f"[INFO] URL: {url}")

    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if (
            href.startswith("https://archive.swot.podaac.earthdata.nasa.gov/")
            and href.lower().endswith(".zip")
        ):
            filename = get_filename_from_url(href)

            # Saltar Node
            if is_node_name(filename):
                continue

            links.append(href)

    return list(dict.fromkeys(links))


def get_output_folder(root: Path, date_obj: datetime) -> Path:
    return root / date_obj.strftime("%Y") / date_obj.strftime("%m") / date_obj.strftime("%d")

def get_filename_from_url(url: str) -> str:
    return Path(urlparse(url).path).name

def delete_existing_node_files(root: Path) -> tuple[int, int]:
    """
    Borra archivos que contengan 'Node' en el nombre dentro del árbol root.
    Retorna (cantidad_borrados, errores).
    """
    deleted = 0
    errors = 0

    print(f"\n[INFO] Buscando archivos 'Node' ya descargados en: {root}")

    for path in root.rglob("*"):
        if path.is_file() and is_node_name(path.name):
            try:
                path.unlink()
                deleted += 1
                print(f"  [DEL]  {path}")
            except Exception as e:
                errors += 1
                print(f"  [ERR]  No se pudo borrar {path}: {e}")

    print(f"[INFO] Archivos Node borrados: {deleted}")
    if errors > 0:
        print(f"[WARN] Errores al borrar Node: {errors}")

    return deleted, errors


def download_file(
    session: requests.Session,
    file_url: str,
    out_path: Path,
    overwrite: bool = False,
) -> str:
    if out_path.exists() and not overwrite:
        print(f"  [SKIP] {out_path.name}")
        return "skipped"

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    try:
        with session.get(file_url, stream=True, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        if out_path.exists():
            out_path.unlink()

        tmp_path.replace(out_path)
        print(f"  [OK]   {out_path.name}")
        return "downloaded"

    except Exception as e:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        print(f"  [ERR]  {out_path.name} -> {e}")
        return "error"

"""
def download_swot_virtual_directory(
    start_date: str,
    end_date: str,
    output_root: str | Path,
    overwrite_existing: bool = False,
    earthdata_token: str = "",
):
    session = build_session(earthdata_token)

    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    output_root = Path(output_root)

    total_found = 0
    total_downloaded = 0
    total_skipped = 0
    total_errors = 0

    for day in date_range(start_dt, end_dt):
        try:
            links = get_download_links_for_day(session, day)
            print(f"[INFO] Archivos encontrados: {len(links)}")
        except Exception as e:
            print(f"[ERROR] No se pudo leer la fecha {day.strftime('%Y-%m-%d')}: {e}")
            total_errors += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        day_folder = get_output_folder(output_root, day)
        day_folder.mkdir(parents=True, exist_ok=True)

        for file_url in links:
            total_found += 1
            filename = get_filename_from_url(file_url)
            out_path = day_folder / filename

            status = download_file(
                session=session,
                file_url=file_url,
                out_path=out_path,
                overwrite=overwrite_existing,
            )

            if status == "downloaded":
                total_downloaded += 1
            elif status == "skipped":
                total_skipped += 1
            else:
                total_errors += 1

            time.sleep(SLEEP_BETWEEN_REQUESTS)

    print("\n" + "=" * 60)
    print("RESUMEN FINAL")
    print("=" * 60)
    print(f"Total enlaces encontrados : {total_found}")
    print(f"Total descargados         : {total_downloaded}")
    print(f"Total saltados            : {total_skipped}")
    print(f"Total errores             : {total_errors}")
    print("=" * 60)
"""

def download_swot_virtual_directory(
    start_date: str,
    end_date: str,
    output_root: str | Path,
    overwrite_existing: bool = False,
    earthdata_token: str = "",
    delete_existing_nodes: bool = True,
):
    session = build_session(earthdata_token)

    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    output_root = Path(output_root)

    total_found = 0
    total_downloaded = 0
    total_skipped = 0
    total_errors = 0
    total_deleted_nodes = 0
    total_delete_errors = 0

    if delete_existing_nodes and output_root.exists():
        total_deleted_nodes, total_delete_errors = delete_existing_node_files(output_root)

    for day in date_range(start_dt, end_dt):
        try:
            links = get_download_links_for_day(session, day)
            print(f"[INFO] Archivos Reach encontrados: {len(links)}")
        except Exception as e:
            print(f"[ERROR] No se pudo leer la fecha {day.strftime('%Y-%m-%d')}: {e}")
            total_errors += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        day_folder = get_output_folder(output_root, day)
        day_folder.mkdir(parents=True, exist_ok=True)

        for file_url in links:
            total_found += 1
            filename = get_filename_from_url(file_url)
            out_path = day_folder / filename

            status = download_file(
                session=session,
                file_url=file_url,
                out_path=out_path,
                overwrite=overwrite_existing,
            )

            if status == "downloaded":
                total_downloaded += 1
            elif status == "skipped":
                total_skipped += 1
            else:
                total_errors += 1

            time.sleep(SLEEP_BETWEEN_REQUESTS)

    print("\n" + "=" * 60)
    print("RESUMEN FINAL")
    print("=" * 60)
    print(f"Archivos Node borrados     : {total_deleted_nodes}")
    print(f"Errores al borrar Node     : {total_delete_errors}")
    print(f"Total Reach encontrados    : {total_found}")
    print(f"Total descargados          : {total_downloaded}")
    print(f"Total saltados             : {total_skipped}")
    print(f"Total errores              : {total_errors}")
    print("=" * 60)


if __name__ == "__main__":
    download_swot_virtual_directory(
        start_date=START_DATE,
        end_date=END_DATE,
        output_root=OUTPUT_ROOT,
        overwrite_existing=OVERWRITE_EXISTING,
        earthdata_token=EARTHDATA_TOKEN,
        delete_existing_nodes=DELETE_EXISTING_NODE_FILES,
    )