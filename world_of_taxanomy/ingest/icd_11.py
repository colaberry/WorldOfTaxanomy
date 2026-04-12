"""ICD-11 MMS (Mortality and Morbidity Statistics) ingester.

Source: WHO ICD-11 API (free registration required)
  https://icd.who.int/icdapi

To obtain the data file:
  1. Register at https://icd.who.int/icdapi (free account)
  2. Download the ICD-11 MMS linearization
  3. Save as data/icd_11.csv with columns:
     Code, Title, ParentCode  (ParentCode is empty for chapter-level nodes)

License: CC BY-ND 3.0 IGO
  Attribution: World Health Organization
  No derivatives permitted.

~35,000-55,000 MMS codes depending on release version.

Hierarchy: ICD-11 codes are alphanumeric (e.g. '1A00', '8B92.2').
Level and parent are read directly from the CSV - NOT derived from code format.
Leaf detection: nodes with no children in the dataset are marked is_leaf=True.
"""
from __future__ import annotations

import csv
from typing import Optional

_DEFAULT_PATH = "data/icd_11.csv"

CHUNK = 500

_SYSTEM_ROW = (
    "icd_11",
    "ICD-11 MMS",
    "International Classification of Diseases 11th Revision - Mortality and Morbidity Statistics",
    "2024-01",
    "Global",
    "World Health Organization",
)


def _parse_level(depth: int) -> int:
    """Convert tree depth (0=root) to 1-indexed level.

    depth 0 -> level 1 (chapter)
    depth 1 -> level 2
    ...
    """
    return depth + 1


def _parse_sector(code: str, parent_map: dict[str, Optional[str]]) -> str:
    """Return the root ancestor code (chapter) for any node.

    Walks the parent_map chain until a node with no parent is found.
    Returns code itself if it has no parent (is a chapter).
    """
    current = code
    visited: set[str] = set()
    while True:
        parent = parent_map.get(current)
        if parent is None:
            return current
        if parent in visited:
            return current  # cycle guard
        visited.add(current)
        current = parent


async def ingest_icd_11(conn, path: Optional[str] = None) -> int:
    """Ingest ICD-11 MMS into classification_system + classification_node.

    Expects a CSV with columns: Code, Title, ParentCode
    ParentCode is empty for chapter-level (root) nodes.

    Computes level from depth in parent chain.
    Leaf detection: nodes that never appear as ParentCode.

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

    # First pass: collect all rows and build parent map
    rows_raw: list[tuple[str, str, Optional[str]]] = []  # (code, title, parent_or_None)

    with open(local, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row.get("Code", "").strip()
            title = row.get("Title", "").strip()
            parent_raw = row.get("ParentCode", "").strip()
            parent = parent_raw if parent_raw else None

            if not code:
                continue

            rows_raw.append((code, title, parent))

    # Build parent map and children set
    parent_map: dict[str, Optional[str]] = {code: parent for code, _, parent in rows_raw}
    has_children: set[str] = {parent for _, _, parent in rows_raw if parent is not None}

    # Build depth map (iterative, safe for deep trees)
    depth_map: dict[str, int] = {}

    def _get_depth(code: str) -> int:
        if code in depth_map:
            return depth_map[code]
        parent = parent_map.get(code)
        if parent is None:
            depth_map[code] = 0
            return 0
        d = _get_depth(parent) + 1
        depth_map[code] = d
        return d

    # Pre-compute depths with iterative approach to avoid recursion limit
    for code, _, _ in rows_raw:
        if code not in depth_map:
            # Walk chain iteratively
            chain = []
            current = code
            while current is not None and current not in depth_map:
                chain.append(current)
                current = parent_map.get(current)
            base_depth = depth_map.get(current, 0) if current is not None else 0
            for i, c in enumerate(reversed(chain)):
                depth_map[c] = base_depth + i + (1 if current is not None else 0)
            # Fix: if current is None, chain[0] is a root, depth=0
            if current is None and chain:
                # chain[-1] (last appended) is the root
                root_idx = len(chain) - 1
                for i, c in enumerate(reversed(chain)):
                    depth_map[c] = i

    records = []
    for code, title, parent in rows_raw:
        depth = depth_map.get(code, 0)
        level = _parse_level(depth)
        sector = _parse_sector(code, parent_map)
        is_leaf = code not in has_children

        records.append((
            "icd_11",
            code,
            title,
            level,
            parent,
            sector,
            is_leaf,
        ))

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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'icd_11'",
        count,
    )

    return count
