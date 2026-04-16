"""Ingest Irrigation Method Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_irrigation", "Irrigation Method", "Irrigation Method Types", "1.0", "Global", "FAO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IG", "Irrigation Method Types", 1, None),
    ("IG.01", "Surface Irrigation (Flood)", 2, 'IG'),
    ("IG.02", "Furrow Irrigation", 2, 'IG'),
    ("IG.03", "Drip Irrigation", 2, 'IG'),
    ("IG.04", "Sprinkler Irrigation", 2, 'IG'),
    ("IG.05", "Center Pivot Irrigation", 2, 'IG'),
    ("IG.06", "Subsurface Drip", 2, 'IG'),
    ("IG.07", "Micro-Sprinkler", 2, 'IG'),
    ("IG.08", "Deficit Irrigation", 2, 'IG'),
    ("IG.09", "Supplemental Irrigation", 2, 'IG'),
    ("IG.10", "Precision Irrigation (Sensor)", 2, 'IG'),
    ("IG.11", "Rainwater Harvesting", 2, 'IG'),
    ("IG.12", "Recycled Water Irrigation", 2, 'IG'),
    ("IG.13", "Aquifer Recharge", 2, 'IG'),
]

async def ingest_domain_irrigation(conn) -> int:
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
