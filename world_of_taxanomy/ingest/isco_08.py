"""ISCO-08 ingester.

International Standard Classification of Occupations (ISCO-08), ILO.
619 nodes across 4 levels:
  - 10 Major Groups     (1-digit code, level 1, e.g. "1" = Managers)
  - 43 Sub-major Groups (2-digit code, level 2, e.g. "11" = Chief Executives...)
  - 130 Minor Groups    (3-digit code, level 3, e.g. "111" = Legislators...)
  - 436 Unit Groups     (4-digit code, level 4, leaf)

Source: ILO International Labour Organization (public access)
  https://webapps.ilo.org/ilostat-files/ISCO/newdocs-08-2021/ISCO-08/ISCO-08%20EN.csv

CSV format (flat - each row is a unit group with all parent columns):
  ISCO_version, major, major_label, sub_major, sub_major_label,
  minor, minor_label, unit, description
File encoding: latin-1
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from world_of_taxanomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = (
    "https://webapps.ilo.org/ilostat-files/ISCO/newdocs-08-2021/"
    "ISCO-08/ISCO-08%20EN.csv"
)
_DEFAULT_PATH = "data/isco_08.csv"

CHUNK = 500

_SYSTEM_ROW = (
    "isco_08",
    "ISCO-08",
    "International Standard Classification of Occupations 2008",
    "2008",
    "Global",
    "International Labour Organization",
)


def _determine_level(code: str) -> int:
    """Return hierarchy level from code length (1-4 digits)."""
    return len(code)


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for Major Groups (1-digit)."""
    if len(code) <= 1:
        return None
    return code[:-1]


def _determine_sector(code: str) -> str:
    """Return the Major Group code (first digit) for any node."""
    return code[0]


async def ingest_isco_08(conn, path: Optional[str] = None) -> int:
    """Ingest ISCO-08 into classification_system + classification_node.

    Reads a flat CSV where each row represents one unit group and carries
    all parent-level columns. All four levels are inserted as distinct nodes
    (deduplicated via ON CONFLICT DO NOTHING). Only rows with
    ISCO_version='ISCO-08' are processed.

    Returns total nodes inserted (or already present on re-run).
    """
    local = path or _DEFAULT_PATH
    ensure_data_file(_DOWNLOAD_URL, local)

    # Register system
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    # Collect unique nodes across all 4 levels
    seen: dict[str, tuple] = {}  # code -> (code, title, level, parent, sector, is_leaf)

    with open(local, newline="", encoding="latin-1") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row["ISCO_version"].strip() != "ISCO-08":
                continue

            major_code = str(row["major"]).strip()
            major_label = row["major_label"].strip()
            sub_code = str(row["sub_major"]).strip()
            sub_label = row["sub_major_label"].strip()
            minor_code = str(row["minor"]).strip()
            minor_label = row["minor_label"].strip()
            unit_code = str(row["unit"]).strip()
            unit_desc = row["description"].strip()

            if major_code and major_code not in seen:
                seen[major_code] = (
                    major_code, major_label,
                    1, None, major_code[0], False,
                )
            if sub_code and sub_code not in seen:
                seen[sub_code] = (
                    sub_code, sub_label,
                    2, sub_code[0], sub_code[0], False,
                )
            if minor_code and minor_code not in seen:
                seen[minor_code] = (
                    minor_code, minor_label,
                    3, minor_code[:2], minor_code[0], False,
                )
            if unit_code and unit_code not in seen:
                seen[unit_code] = (
                    unit_code, unit_desc,
                    4, unit_code[:3], unit_code[0], True,
                )

    records = [
        ("isco_08", code, title, level, parent, sector, is_leaf)
        for code, title, level, parent, sector, is_leaf in seen.values()
    ]

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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'isco_08'",
        count,
    )

    return count
