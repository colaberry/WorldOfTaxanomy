"""Ingest Mangrove Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_mangrove", "Mangrove", "Mangrove Types", "1.0", "Global", "IUCN")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MN", "Mangrove Types", 1, None),
    ("MN.01", "Red Mangrove", 2, 'MN'),
    ("MN.02", "Black Mangrove", 2, 'MN'),
    ("MN.03", "White Mangrove", 2, 'MN'),
    ("MN.04", "Buttonwood Mangrove", 2, 'MN'),
    ("MN.05", "Riverine Mangrove", 2, 'MN'),
    ("MN.06", "Fringe Mangrove", 2, 'MN'),
    ("MN.07", "Basin Mangrove", 2, 'MN'),
    ("MN.08", "Overwash Mangrove", 2, 'MN'),
    ("MN.09", "Mangrove Restoration", 2, 'MN'),
    ("MN.10", "Blue Carbon (Mangrove)", 2, 'MN'),
    ("MN.11", "Mangrove Mapping (Remote Sensing)", 2, 'MN'),
    ("MN.12", "Mangrove Ecosystem Services", 2, 'MN'),
]

async def ingest_domain_mangrove(conn) -> int:
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
