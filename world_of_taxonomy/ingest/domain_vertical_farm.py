"""Ingest Vertical Farming Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_vertical_farm", "Vertical Farming", "Vertical Farming Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("VF", "Vertical Farming Types", 1, None),
    ("VF.01", "Indoor Vertical Farm", 2, 'VF'),
    ("VF.02", "Container Farm (Shipping)", 2, 'VF'),
    ("VF.03", "Warehouse Farm", 2, 'VF'),
    ("VF.04", "Rooftop Farm", 2, 'VF'),
    ("VF.05", "LED Grow Light System", 2, 'VF'),
    ("VF.06", "Hydroponic Vertical Farm", 2, 'VF'),
    ("VF.07", "Aeroponic Vertical Farm", 2, 'VF'),
    ("VF.08", "Nutrient Delivery System", 2, 'VF'),
    ("VF.09", "Climate Control (HVAC)", 2, 'VF'),
    ("VF.10", "Automation and Robotics", 2, 'VF'),
    ("VF.11", "Crop Selection (Leafy Greens)", 2, 'VF'),
    ("VF.12", "Energy Efficiency Strategy", 2, 'VF'),
]

async def ingest_domain_vertical_farm(conn) -> int:
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
