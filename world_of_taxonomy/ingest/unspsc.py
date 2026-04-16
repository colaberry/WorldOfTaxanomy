"""UNSPSC v24 ingester.

United Nations Standard Products and Services Code (UNSPSC) v24.
77,337 nodes across 4 levels:
  - 57 Segments   (8-digit code ending in 000000, level 1)
  - 465 Families  (8-digit code ending in 0000,   level 2)
  - 5,313 Classes (8-digit code ending in 00,     level 3)
  - 71,502 Commodities (leaf nodes, level 4)

Source: Oklahoma Open Data Portal (public domain)
  https://data.ok.gov/dataset/unspsc-codes

CSV format (flat, all hierarchy columns on each row):
  Segment, Segment Name, Family, Family Name, Class, Class Name, Commodity, Commodity Name
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from world_of_taxonomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = (
    "https://data.ok.gov/dataset/18a622a6-32d1-48f6-842a-8232bc4ca06c"
    "/resource/b92ad3ac-b0f5-4c62-9bd0-eac023cfd083"
    "/download/data-unspsc-codes.csv"
)
_DEFAULT_PATH = "data/unspsc_v24.csv"

CHUNK = 500

_SYSTEM_ROW = (
    "unspsc_v24",
    "UNSPSC",
    "United Nations Standard Products and Services Code v24",
    "v24",
    "Global",
    "GS1 US",
)


def _determine_level(code: str) -> int:
    """Return hierarchy level from trailing zeros in 8-digit code."""
    if code.endswith("000000"):
        return 1
    if code.endswith("0000"):
        return 2
    if code.endswith("00"):
        return 3
    return 4


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for segments."""
    if code.endswith("000000"):
        return None
    if code.endswith("0000"):
        return code[:2] + "000000"
    if code.endswith("00"):
        return code[:4] + "0000"
    return code[:6] + "00"


def _determine_sector(code: str) -> str:
    """Return the segment code (level-1 ancestor) for any node."""
    return code[:2] + "000000"


async def ingest_unspsc(conn, path: Optional[str] = None) -> int:
    """Ingest UNSPSC v24 into classification_system + classification_node.

    Reads a flat CSV where each row represents one commodity and also carries
    its Segment, Family, and Class columns. All four levels are inserted as
    distinct nodes (deduplicated via ON CONFLICT DO NOTHING).

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

    # Collect all unique nodes across all 4 levels
    seen: dict[str, tuple] = {}  # code -> (code, title, level, parent, sector, is_leaf)

    with open(local, newline="", encoding="latin-1") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            seg_code = row["Segment"].strip()
            seg_name = row["Segment Name"].strip()
            fam_code = row["Family"].strip()
            fam_name = row["Family Name"].strip()
            cls_code = row["Class"].strip()
            cls_name = row["Class Name"].strip()
            com_code = row["Commodity"].strip()
            com_name = row["Commodity Name"].strip()

            if seg_code and seg_code not in seen:
                seen[seg_code] = (
                    seg_code, seg_name,
                    1, None, _determine_sector(seg_code), False,
                )
            if fam_code and fam_code not in seen:
                seen[fam_code] = (
                    fam_code, fam_name,
                    2, _determine_parent(fam_code), _determine_sector(fam_code), False,
                )
            if cls_code and cls_code not in seen:
                seen[cls_code] = (
                    cls_code, cls_name,
                    3, _determine_parent(cls_code), _determine_sector(cls_code), False,
                )
            if com_code and com_code not in seen:
                seen[com_code] = (
                    com_code, com_name,
                    4, _determine_parent(com_code), _determine_sector(com_code), True,
                )

    records = [
        ("unspsc_v24", code, title, level, parent, sector, is_leaf)
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

    # Update node_count
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'unspsc_v24'",
        count,
    )

    return count
