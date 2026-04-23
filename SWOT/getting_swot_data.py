from __future__ import annotations

import os
import earthaccess
from pathlib import Path
from datetime import datetime, timezone

# -----------------------------
# Config
# -----------------------------
SHORT_NAME = "SWOT_L2_HR_RiverSP_reach_2.0"  # reaches only (shapefile)
USER_START = "2020-01-01T00:00:00Z"
OUT_DIR = Path("./swot_riversp_reach_downloads").resolve()

# Dataset start date per PO.DAAC page (cannot download before this)
DATASET_START = "2022-12-16T00:00:00Z"  # Start/Stop Date 2022-Dec-16 to Present
# -----------------------------


def iso_now_utc_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def max_iso_z(a: str, b: str) -> str:
    # both like "YYYY-mm-ddTHH:MM:SSZ"
    da = datetime.fromisoformat(a.replace("Z", "+00:00"))
    db = datetime.fromisoformat(b.replace("Z", "+00:00"))
    return a if da >= db else b


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    end = iso_now_utc_z()
    start = max_iso_z(USER_START, DATASET_START)

    print("Requested range :", USER_START, "→", end)
    if start != USER_START:
        print("NOTE: Clipped start:", start, "(dataset has no data before 2022-12-16)")

    # Login (expects .netrc or interactive)
    auth = earthaccess.login(strategy="netrc")
    if not auth:
        raise RuntimeError("Earthaccess login failed. Configure .netrc (recommended) or login interactively.")

    # Search granules
    results = earthaccess.search_data(
        short_name=SHORT_NAME,
        temporal=(start, end),
    )

    print(f"\nGranules found: {len(results):,}")
    if len(results) == 0:
        print("Nothing to download for this time range.")
        return

    # Preview what will be downloaded (titles / filenames)
    print("\nPreview (first 20 granules):")
    for g in results[:20]:
        # granule metadata varies; 'title' is commonly available
        title = getattr(g, "title", None) or g.get("title", "<no title>")
        print(" -", title)

    # OPTIONAL: show approximate unique file links count (depends on provider)
    links = earthaccess.get_links(results)
    print(f"\nFile links to download (from CMR): {len(links):,}")
    print("Preview links (first 10):")
    for u in links[:10]:
        print(" -", u)

    # Download all
    print(f"\nDownloading to: {OUT_DIR}")
    earthaccess.download(results, local_path=str(OUT_DIR))
    print("\nDone.")


if __name__ == "__main__":
    main()
