"""LOINC ingester.

Logical Observation Identifiers Names and Codes (LOINC).
Source: Regenstrief Institute - loinc.org (free registration required)
  https://loinc.org/downloads/loinc-table/
  Download: Loinc_X.XX.zip -> place at data/Loinc_X.XX.zip
  OR extract Loinc.csv and save as data/loinc.csv

License: Regenstrief LOINC License
  DO NOT redistribute the data file.
  Auto-download is not supported due to license restrictions.

LOINC codes are flat (no parent hierarchy). All codes are:
  - level = 1
  - parent_code = None
  - is_leaf = True
  - sector_code = CLASSTYPE (1=Lab, 2=Clinical, 3=Claim attachments, 4=Survey)

Only ACTIVE and TRIAL codes are ingested (DEPRECATED and DISCOURAGED are skipped).
~100,000 codes in recent releases.
"""
from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from typing import Optional

_DEFAULT_CSV_PATH = "data/loinc.csv"
_DEFAULT_ZIP_GLOB = "data/Loinc_*.zip"

CHUNK = 500

_SYSTEM_ROW = (
    "loinc",
    "LOINC",
    "Logical Observation Identifiers Names and Codes",
    "2.82",
    "Global",
    "Regenstrief Institute",
)

# CLASSTYPE numeric to sector code
_CLASSTYPE_SECTOR = {
    "1": "LAB",
    "2": "CLINICAL",
    "3": "CLAIMS",
    "4": "SURVEY",
}


def _is_active(status: str) -> bool:
    """Return True if the LOINC code should be ingested.

    ACTIVE and TRIAL codes are included.
    DEPRECATED and DISCOURAGED codes are excluded.
    Empty status is excluded.
    """
    s = status.strip().upper()
    if not s:
        return False
    return s not in ("DEPRECATED", "DISCOURAGED")


def _find_loinc_path() -> Optional[str]:
    """Auto-detect the LOINC data file.

    Checks in order:
      1. data/loinc.csv (extracted CSV)
      2. data/Loinc_*.zip (ZIP downloaded directly from loinc.org)
    Returns the path string, or None if not found.
    """
    csv_path = Path(_DEFAULT_CSV_PATH)
    if csv_path.exists():
        return str(csv_path)
    zips = sorted(Path("data").glob("Loinc_*.zip"))
    if zips:
        return str(zips[-1])  # newest version
    return None


def _open_loinc_csv(path: str):
    """Open a LOINC CSV file from either a plain CSV or a ZIP archive.

    If path ends with .zip, locates LoincTable/Loinc.csv inside the archive.
    Returns an iterable of dict rows (csv.DictReader).
    """
    if path.lower().endswith(".zip"):
        zf = zipfile.ZipFile(path)
        # Find LoincTable/Loinc.csv inside the archive
        member = next(
            (n for n in zf.namelist() if n.endswith("LoincTable/Loinc.csv")),
            None,
        )
        if member is None:
            raise FileNotFoundError(
                f"Could not find LoincTable/Loinc.csv inside {path}"
            )
        raw = zf.open(member)
        text = io.TextIOWrapper(raw, encoding="utf-8-sig")
        return csv.DictReader(text), zf  # caller must close zf
    else:
        fh = open(path, newline="", encoding="utf-8-sig")
        return csv.DictReader(fh), fh


async def ingest_loinc(conn, path: Optional[str] = None) -> int:
    """Ingest LOINC into classification_system + classification_node.

    Reads from path (CSV or ZIP). If path is None, auto-detects:
      - data/loinc.csv   (manually extracted CSV)
      - data/Loinc_*.zip (ZIP downloaded from loinc.org)

    Skips DEPRECATED and DISCOURAGED codes.
    All codes are flat (level=1, no parent, is_leaf=True).

    Returns total nodes inserted (or already present on re-run).
    """
    local = path or _find_loinc_path()
    if local is None:
        raise FileNotFoundError(
            "LOINC data not found. Download from https://loinc.org/downloads/loinc-table/ "
            "(free registration required) and place the ZIP at data/Loinc_X.XX.zip "
            "or extract Loinc.csv to data/loinc.csv"
        )

    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    seen: dict[str, tuple] = {}

    reader, handle = _open_loinc_csv(local)
    try:
        for row in reader:
            code = row.get("LOINC_NUM", "").strip()
            title = row.get("LONG_COMMON_NAME", "").strip() or row.get("COMPONENT", "").strip()
            status = row.get("STATUS", "").strip()
            classtype = row.get("CLASSTYPE", "").strip()

            if not code:
                continue
            if not _is_active(status):
                continue

            sector = _CLASSTYPE_SECTOR.get(classtype, classtype or "1")

            if code not in seen:
                seen[code] = (
                    "loinc",
                    code,
                    title,
                    1,       # level: flat structure
                    None,    # parent_code: none
                    sector,  # sector_code: CLASSTYPE
                    True,    # is_leaf: always True
                )
    finally:
        handle.close()

    records = list(seen.values())

    count = 0
    for i in range(0, len(records), CHUNK):
        chunk = records[i: i + CHUNK]
        await conn.executemany(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (system_id, code) DO NOTHING""",
            chunk,
        )
        count += len(chunk)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'loinc'",
        count,
    )

    return count
