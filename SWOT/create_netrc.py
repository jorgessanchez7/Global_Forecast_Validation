from __future__ import annotations

import os
import getpass
from pathlib import Path


URS = "urs.earthdata.nasa.gov"

# Windows: prefer _netrc, Unix: .netrc
home = Path(os.path.expanduser("~"))
netrc_path = home / ("_netrc" if os.name == "nt" else ".netrc")

uid = input("Earthdata username: ").strip()
pwd = getpass.getpass("Earthdata password (hidden): ")

entry = f"machine {URS}\n  login {uid}\n  password {pwd}\n"

# Preserve other machines if file exists
lines_out = [entry.rstrip("\n")]
if netrc_path.exists():
    existing = netrc_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    skip = False
    i = 0
    # Remove existing block for URS (simple heuristic)
    while i < len(existing):
        line = existing[i].strip()
        if line.startswith("machine ") and URS in line:
            # skip this machine block lines until next "machine" or EOF
            i += 1
            while i < len(existing) and not existing[i].strip().startswith("machine "):
                i += 1
            continue
        lines_out.append(existing[i])
        i += 1

netrc_path.write_text("\n".join(lines_out).strip() + "\n", encoding="utf-8")
print(f"Created/updated: {netrc_path}")
