"""Ingest Software License Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_sw_license", "Software License", "Software License Types", "1.0", "Global", "OSI / FSF")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SL", "Software License Types", 1, None),
    ("SL.01", "MIT License", 2, 'SL'),
    ("SL.02", "Apache License 2.0", 2, 'SL'),
    ("SL.03", "GNU GPL v3", 2, 'SL'),
    ("SL.04", "GNU LGPL v3", 2, 'SL'),
    ("SL.05", "BSD 2-Clause", 2, 'SL'),
    ("SL.06", "BSD 3-Clause", 2, 'SL'),
    ("SL.07", "Mozilla Public License 2.0", 2, 'SL'),
    ("SL.08", "ISC License", 2, 'SL'),
    ("SL.09", "Creative Commons (CC BY)", 2, 'SL'),
    ("SL.10", "Unlicense (Public Domain)", 2, 'SL'),
    ("SL.11", "Eclipse Public License 2.0", 2, 'SL'),
    ("SL.12", "Proprietary (EULA)", 2, 'SL'),
    ("SL.13", "Dual Licensing", 2, 'SL'),
    ("SL.14", "AGPL v3", 2, 'SL'),
    ("SL.15", "SSPL (Server Side Public)", 2, 'SL'),
    ("SL.16", "BSL (Business Source)", 2, 'SL'),
]

async def ingest_domain_sw_license(conn) -> int:
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
