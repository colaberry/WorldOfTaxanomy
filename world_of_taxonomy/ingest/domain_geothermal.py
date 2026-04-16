"""Ingest Geothermal System Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_geothermal", "Geothermal System", "Geothermal System Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GE", "Geothermal System Types", 1, None),
    ("GE.01", "Dry Steam Plant", 2, 'GE'),
    ("GE.02", "Flash Steam Plant", 2, 'GE'),
    ("GE.03", "Binary Cycle Plant", 2, 'GE'),
    ("GE.04", "Enhanced Geothermal System (EGS)", 2, 'GE'),
    ("GE.05", "Ground Source Heat Pump", 2, 'GE'),
    ("GE.06", "Direct Use Application", 2, 'GE'),
    ("GE.07", "Geothermal District Heating", 2, 'GE'),
    ("GE.08", "Sedimentary Basin Geothermal", 2, 'GE'),
    ("GE.09", "Volcanic Geothermal", 2, 'GE'),
    ("GE.10", "Hot Dry Rock", 2, 'GE'),
    ("GE.11", "Geopressured Geothermal", 2, 'GE'),
    ("GE.12", "Supercritical Geothermal", 2, 'GE'),
    ("GE.13", "Closed-Loop Geothermal", 2, 'GE'),
]

async def ingest_domain_geothermal(conn) -> int:
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
