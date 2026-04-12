"""CIP 2020 ingester.

Classification of Instructional Programs (CIP) 2020, NCES.
2,848 nodes across 3 levels:
  - 50 Families  (2-digit, no period,  level 1, e.g. "01")
  - 473 Areas    (XX.XX format,        level 2, e.g. "01.01")
  - 2,325 Programs (XX.XXXX format,    level 3, leaf)

Source: National Center for Education Statistics (public domain)
  https://nces.ed.gov/ipeds/cipcode/Files/CIPCode2020.csv

CSV format: codes use Excel-escape prefix ='XX' which is stripped at parse time.
Columns: CIPFamily, CIPCode, Action, TextChange, CIPTitle, CIPDefinition, ...
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Optional

from world_of_taxanomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = "https://nces.ed.gov/ipeds/cipcode/Files/CIPCode2020.csv"
_DEFAULT_PATH = "data/cip_2020.csv"

CHUNK = 500

_SYSTEM_ROW = (
    "cip_2020",
    "CIP 2020",
    "Classification of Instructional Programs 2020",
    "2020",
    "United States",
    "National Center for Education Statistics",
)


def _clean_code(raw: str) -> str:
    """Strip Excel-escape prefix ='...' from CIP code strings."""
    return re.sub(r'^=?"?|"$', "", raw).strip()


def _determine_level(code: str) -> int:
    """Return hierarchy level from code format.

    - No period: Family (level 1)
    - XX.XX (2 digits after period): Area (level 2)
    - XX.XXXX (4 digits after period): Program (level 3)
    """
    if "." not in code:
        return 1
    after_dot = code.split(".")[1]
    if len(after_dot) == 2:
        return 2
    return 3


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for Family (level 1)."""
    if "." not in code:
        return None
    before_dot, after_dot = code.split(".", 1)
    if len(after_dot) == 2:
        return before_dot          # Area -> Family
    return before_dot + "." + after_dot[:2]   # Program -> Area


def _determine_sector(code: str) -> str:
    """Return the Family code (first 2 digits) for any node."""
    return code[:2]


async def ingest_cip_2020(conn, path: Optional[str] = None) -> int:
    """Ingest CIP 2020 into classification_system + classification_node.

    Rows with Action='Deleted' are skipped. Families, Areas, and Programs
    are all stored as separate nodes (deduplicated via ON CONFLICT DO NOTHING).

    Returns total nodes inserted (or already present on re-run).
    """
    local = path or _DEFAULT_PATH
    ensure_data_file(_DOWNLOAD_URL, local)

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
            code = _clean_code(row["CIPCode"])
            title = row["CIPTitle"].strip().rstrip(".")
            action = row.get("Action", "").strip().lower()

            if not code or action == "deleted":
                continue

            level = _determine_level(code)
            parent = _determine_parent(code)
            sector = _determine_sector(code)
            is_leaf = level == 3

            if code not in seen:
                seen[code] = ("cip_2020", code, title, level, parent, sector, is_leaf)

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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'cip_2020'",
        count,
    )

    return count
