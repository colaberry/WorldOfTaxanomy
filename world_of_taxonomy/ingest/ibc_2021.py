"""Ingest International Building Code 2021."""
from __future__ import annotations

_SYSTEM_ROW = ("ibc_2021", "IBC 2021", "International Building Code 2021", "2021", "Global", "ICC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IB", "IBC Chapters", 1, None),
    ("IB.01", "Scope and Administration", 2, 'IB'),
    ("IB.02", "Definitions", 2, 'IB'),
    ("IB.03", "Use and Occupancy Classification", 2, 'IB'),
    ("IB.04", "Special Detailed Hazards", 2, 'IB'),
    ("IB.05", "Building Heights and Areas", 2, 'IB'),
    ("IB.06", "Types of Construction", 2, 'IB'),
    ("IB.07", "Fire and Smoke Protection", 2, 'IB'),
    ("IB.08", "Interior Finishes", 2, 'IB'),
    ("IB.09", "Fire Protection Systems", 2, 'IB'),
    ("IB.10", "Means of Egress", 2, 'IB'),
    ("IB.11", "Accessibility", 2, 'IB'),
    ("IB.12", "Interior Environment", 2, 'IB'),
    ("IB.13", "Energy Efficiency", 2, 'IB'),
    ("IB.14", "Exterior Walls", 2, 'IB'),
    ("IB.15", "Roof Assemblies", 2, 'IB'),
    ("IB.16", "Structural Design", 2, 'IB'),
    ("IB.17", "Soils and Foundations", 2, 'IB'),
    ("IB.18", "Concrete", 2, 'IB'),
    ("IB.19", "Steel", 2, 'IB'),
    ("IB.20", "Wood", 2, 'IB'),
    ("IB.21", "Masonry", 2, 'IB'),
    ("IB.22", "Plumbing", 2, 'IB'),
    ("IB.23", "Mechanical", 2, 'IB'),
    ("IB.24", "Electrical", 2, 'IB'),
    ("IB.25", "Elevators and Escalators", 2, 'IB'),
]

async def ingest_ibc_2021(conn) -> int:
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
