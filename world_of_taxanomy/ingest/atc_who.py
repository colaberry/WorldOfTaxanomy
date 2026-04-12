"""ATC WHO Drug Classification ingester.

Anatomical Therapeutic Chemical (ATC) classification, WHO 2021.
Source: github.com/fabkury/atcd (CC BY 4.0)
  https://raw.githubusercontent.com/fabkury/atcd/master/WHO%20ATC-DDD%202021-12-03.csv

Hierarchy (level by code length):
  L1 - Anatomical main group    (1-char,  e.g. "A")
  L2 - Therapeutic subgroup     (3-char,  e.g. "A01")
  L3 - Pharmacological subgroup (4-char,  e.g. "A01A")
  L4 - Chemical subgroup        (5-char,  e.g. "A01AA")
  L5 - Chemical substance       (7-char,  leaf, e.g. "A01AA01")

14 + 94 + 269 + 910 + 5684 = 6971 nodes.
"""
from __future__ import annotations

import csv
from typing import Optional

from world_of_taxanomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = (
    "https://raw.githubusercontent.com/fabkury/atcd/master/"
    "WHO%20ATC-DDD%202021-12-03.csv"
)
_DEFAULT_PATH = "data/atc_who.csv"

CHUNK = 500

_SYSTEM_ROW = (
    "atc_who",
    "ATC WHO 2021",
    "Anatomical Therapeutic Chemical Classification System (WHO 2021)",
    "2021",
    "Global",
    "World Health Organization",
)

_LEVEL_BY_LEN = {1: 1, 3: 2, 4: 3, 5: 4, 7: 5}


def _determine_level(code: str) -> int:
    """Return hierarchy level from code length.

    1-char  -> L1 (Anatomical main group)
    3-char  -> L2 (Therapeutic subgroup)
    4-char  -> L3 (Pharmacological subgroup)
    5-char  -> L4 (Chemical subgroup)
    7-char  -> L5 (Chemical substance, leaf)
    """
    return _LEVEL_BY_LEN[len(code)]


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for level-1 (single char) codes."""
    n = len(code)
    if n == 1:
        return None
    if n == 3:
        return code[:1]
    if n == 4:
        return code[:3]
    if n == 5:
        return code[:4]
    return code[:5]  # 7-char -> 5-char parent


def _determine_sector(code: str) -> str:
    """Return the anatomical main group code (first char) for any node."""
    return code[0]


async def ingest_atc_who(conn, path: Optional[str] = None) -> int:
    """Ingest ATC WHO 2021 into classification_system + classification_node.

    Reads fabkury/atcd CSV. Level-5 nodes (7-char codes) are leaves.
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

    with open(local, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row["atc_code"].strip()
            title = row["atc_name"].strip()

            if not code or len(code) not in _LEVEL_BY_LEN:
                continue

            if code not in seen:
                level = _determine_level(code)
                is_leaf = level == 5
                seen[code] = (
                    "atc_who",
                    code,
                    title,
                    level,
                    _determine_parent(code),
                    _determine_sector(code),
                    is_leaf,
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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'atc_who'",
        count,
    )

    return count
