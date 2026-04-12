"""SOC 2018 ingester.

Standard Occupational Classification (SOC) 2018, US Bureau of Labor Statistics.
1,447 nodes across 4 levels:
  - 23 Major Groups       (code ends "0000",  level 1, e.g. "11-0000")
  - 98 Minor Groups       (code ends "000",   level 2, e.g. "11-1000")
  - 459 Broad Occupations (code ends "0",     level 3, e.g. "11-1010")
  - 867 Detailed Occ.     (no trailing "0",   level 4, leaf)

O*NET extension codes (e.g. "11-1011.03") are skipped.

Source: Budget Lab at Yale (mirrors BLS SOC 2018, public domain)
  https://raw.githubusercontent.com/Budget-Lab-Yale/AI-Employment-Model/main/resources/SOC_Structure.csv
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from world_of_taxanomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = (
    "https://raw.githubusercontent.com/Budget-Lab-Yale/AI-Employment-Model"
    "/main/resources/SOC_Structure.csv"
)
_DEFAULT_PATH = "data/soc_2018.csv"

CHUNK = 500

_SYSTEM_ROW = (
    "soc_2018",
    "SOC 2018",
    "Standard Occupational Classification 2018",
    "2018",
    "United States",
    "US Bureau of Labor Statistics",
)


def _determine_level(code: str) -> int:
    """Return hierarchy level from trailing zeros in 'XX-XXXX' SOC code."""
    if code.endswith("0000"):
        return 1
    if code.endswith("000"):
        return 2
    if code.endswith("0"):
        return 3
    return 4


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for Major Groups."""
    if code.endswith("0000"):
        return None
    prefix = code[:3]  # "11-"
    if code.endswith("000"):
        return prefix + "0000"
    if code.endswith("0"):
        return prefix + code[3] + "000"
    return code[:6] + "0"


def _determine_sector(code: str) -> str:
    """Return the Major Group code (level-1 ancestor) for any node."""
    return code[:3] + "0000"


async def ingest_soc_2018(conn, path: Optional[str] = None) -> int:
    """Ingest SOC 2018 into classification_system + classification_node.

    Parses a columnar CSV where each row has exactly one level populated.
    O*NET extension rows (non-empty 'Detailed O*NET-SOC' column) are skipped.

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

    records: list[tuple] = []

    with open(local, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # Skip O*NET extension rows
            if row["Detailed O*NET-SOC"].strip():
                continue

            title = row["SOC or O*NET-SOC 2019 Title"].strip()

            if row["Major Group"].strip():
                code = row["Major Group"].strip()
                is_leaf = False
            elif row["Minor Group"].strip():
                code = row["Minor Group"].strip()
                is_leaf = False
            elif row["Broad Occupation"].strip():
                code = row["Broad Occupation"].strip()
                is_leaf = False
            elif row["Detailed Occupation"].strip():
                code = row["Detailed Occupation"].strip()
                is_leaf = True
            else:
                continue

            level = _determine_level(code)
            parent = _determine_parent(code)
            sector = _determine_sector(code)
            records.append(("soc_2018", code, title, level, parent, sector, is_leaf))

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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'soc_2018'",
        count,
    )

    return count
