"""ISCED-F 2013 (Fields of Education and Training) ingester.

Source: united-education/isced GitHub (JSON, public domain)
  https://raw.githubusercontent.com/united-education/isced/master/isced.json

Hierarchy (level determined by code length):
  L1 - Broad field   (2-digit, e.g. "01" = Education)
  L2 - Narrow field  (3-digit, e.g. "011" = Education)
  L3 - Detailed field (4-digit, leaf, e.g. "0111" = Education science)

11 broad + 30 narrow + 81 detailed = 122 nodes.
"""
from __future__ import annotations

import json
import re
from typing import Optional

from world_of_taxanomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = (
    "https://raw.githubusercontent.com/united-education/isced/master/isced.json"
)
_DEFAULT_PATH = "data/iscedf_2013.json"

CHUNK = 500

_SYSTEM_ROW = (
    "iscedf_2013",
    "ISCED-F 2013",
    "International Standard Classification of Education: Fields of Education and Training 2013",
    "2013",
    "Global",
    "UNESCO Institute for Statistics",
)


def _determine_level(code: str) -> int:
    """Return hierarchy level from code length.

    - 2-digit: Broad field (level 1)
    - 3-digit: Narrow field (level 2)
    - 4-digit: Detailed field (level 3, leaf)
    """
    return len(code) - 1


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for broad fields (2-digit)."""
    if len(code) == 2:
        return None
    return code[:-1]


def _determine_sector(code: str) -> str:
    """Return the broad field code (first 2 digits) for any node."""
    return code[:2]


def _fix_json(raw: str) -> str:
    """Remove trailing commas from JSON (common in hand-written JSON)."""
    return re.sub(r",\s*([}\]])", r"\1", raw)


async def ingest_iscedf_2013(conn, path: Optional[str] = None) -> int:
    """Ingest ISCED-F 2013 into classification_system + classification_node.

    Parses the united-education/isced JSON which nests broad -> narrow -> detailed.
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

    with open(local, encoding="utf-8") as fh:
        raw = fh.read()

    data = json.loads(_fix_json(raw))

    nodes: list[tuple] = []
    seen: set[str] = set()

    def _get_title(title_field) -> str:
        if isinstance(title_field, dict):
            return title_field.get("en", "")
        return str(title_field) if title_field else ""

    for broad_entry in data:
        broad_code = broad_entry["code"]
        broad_title = _get_title(broad_entry.get("title", ""))
        if broad_code not in seen:
            seen.add(broad_code)
            nodes.append((
                "iscedf_2013",
                broad_code,
                broad_title,
                _determine_level(broad_code),
                _determine_parent(broad_code),
                _determine_sector(broad_code),
                False,
            ))

        for narrow_entry in broad_entry.get("items", []):
            narrow_code = narrow_entry["code"]
            narrow_title = _get_title(narrow_entry.get("title", ""))
            if narrow_code not in seen:
                seen.add(narrow_code)
                nodes.append((
                    "iscedf_2013",
                    narrow_code,
                    narrow_title,
                    _determine_level(narrow_code),
                    _determine_parent(narrow_code),
                    _determine_sector(narrow_code),
                    False,
                ))

            for detail_entry in narrow_entry.get("items", []):
                detail_code = detail_entry["code"]
                detail_title = _get_title(detail_entry.get("title", ""))
                if detail_code not in seen:
                    seen.add(detail_code)
                    nodes.append((
                        "iscedf_2013",
                        detail_code,
                        detail_title,
                        _determine_level(detail_code),
                        _determine_parent(detail_code),
                        _determine_sector(detail_code),
                        True,
                    ))

    count = 0
    for i in range(0, len(nodes), CHUNK):
        chunk = nodes[i: i + CHUNK]
        await conn.executemany(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (system_id, code) DO NOTHING""",
            chunk,
        )
        count += len(chunk)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'iscedf_2013'",
        count,
    )

    return count
