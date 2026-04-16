"""Ingest SPDX Software License Identifier Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("spdx_licenses", "SPDX Licenses", "SPDX Software License Identifier Categories", "3.24", "Global", "Linux Foundation")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC0-1.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SL", "SPDX License Categories", 1, None),
    ("SL.01", "Permissive: MIT", 2, 'SL'),
    ("SL.02", "Permissive: Apache-2.0", 2, 'SL'),
    ("SL.03", "Permissive: BSD-2-Clause", 2, 'SL'),
    ("SL.04", "Permissive: BSD-3-Clause", 2, 'SL'),
    ("SL.05", "Permissive: ISC", 2, 'SL'),
    ("SL.06", "Copyleft: GPL-2.0", 2, 'SL'),
    ("SL.07", "Copyleft: GPL-3.0", 2, 'SL'),
    ("SL.08", "Weak Copyleft: LGPL-2.1", 2, 'SL'),
    ("SL.09", "Weak Copyleft: LGPL-3.0", 2, 'SL'),
    ("SL.10", "Weak Copyleft: MPL-2.0", 2, 'SL'),
    ("SL.11", "Network Copyleft: AGPL-3.0", 2, 'SL'),
    ("SL.12", "Creative Commons: CC-BY-4.0", 2, 'SL'),
    ("SL.13", "Creative Commons: CC-BY-SA-4.0", 2, 'SL'),
    ("SL.14", "Public Domain: CC0-1.0", 2, 'SL'),
    ("SL.15", "Public Domain: Unlicense", 2, 'SL'),
    ("SL.16", "Proprietary/Commercial", 2, 'SL'),
]

async def ingest_spdx_licenses(conn) -> int:
    sid, short, full, ver, region, authority = _SYSTEM_ROW
    await conn.execute(
        """INSERT INTO classification_system (id, name, full_name, version, region, authority,
                  source_url, source_date, data_provenance, license)
           VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_DATE,$8,$9)
           ON CONFLICT (id) DO UPDATE SET name=$2,full_name=$3,version=$4,region=$5,
                  authority=$6,source_url=$7,source_date=CURRENT_DATE,
                  data_provenance=$8,license=$9""",
        sid, short, full, ver, region, authority,
        _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute("DELETE FROM classification_node WHERE system_id = $1", sid)
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1,$2,$3,$4,$5)""",
            sid, code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, sid)
    return count
