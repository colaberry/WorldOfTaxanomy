"""LOINC ingester.

Logical Observation Identifiers Names and Codes (LOINC).
Source: Regenstrief Institute - loinc.org (free registration required)
  https://loinc.org/downloads/loinc-table/
  Download: Loinc_X.XX.zip -> extract Loinc.csv -> save as data/loinc.csv

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
from typing import Optional

_DEFAULT_PATH = "data/loinc.csv"

CHUNK = 500

_SYSTEM_ROW = (
    "loinc",
    "LOINC",
    "Logical Observation Identifiers Names and Codes",
    "2.77",
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


async def ingest_loinc(conn, path: Optional[str] = None) -> int:
    """Ingest LOINC into classification_system + classification_node.

    Reads Loinc.csv (manually downloaded from loinc.org).
    Skips DEPRECATED and DISCOURAGED codes.
    All codes are flat (level=1, no parent, is_leaf=True).

    Returns total nodes inserted (or already present on re-run).
    """
    local = path or _DEFAULT_PATH

    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    seen: dict[str, tuple] = {}

    with open(local, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
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
