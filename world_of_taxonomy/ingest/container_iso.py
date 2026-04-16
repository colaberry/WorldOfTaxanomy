"""Ingest ISO Container Type Group Codes (ISO 6346)."""
from __future__ import annotations

_SYSTEM_ROW = ("container_iso", "Container Types ISO", "ISO Container Type Group Codes (ISO 6346)", "2024", "Global", "ISO/BIC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CI", "ISO Container Types", 1, None),
    ("CI.01", "GP - General Purpose", 2, 'CI'),
    ("CI.02", "VH - Ventilated", 2, 'CI'),
    ("CI.03", "BU - Bulk", 2, 'CI'),
    ("CI.04", "RE - Reefer (Refrigerated)", 2, 'CI'),
    ("CI.05", "TN - Tank", 2, 'CI'),
    ("CI.06", "PL - Platform (Flat Rack)", 2, 'CI'),
    ("CI.07", "OT - Open Top", 2, 'CI'),
    ("CI.08", "UT - Named Cargo", 2, 'CI'),
    ("CI.09", "20ft Standard (20GP)", 2, 'CI'),
    ("CI.10", "40ft Standard (40GP)", 2, 'CI'),
    ("CI.11", "40ft High Cube (40HC)", 2, 'CI'),
    ("CI.12", "45ft High Cube (45HC)", 2, 'CI'),
    ("CI.13", "Swap Body", 2, 'CI'),
]

async def ingest_container_iso(conn) -> int:
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
