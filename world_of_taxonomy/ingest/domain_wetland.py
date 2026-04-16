"""Ingest Wetland Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_wetland", "Wetland Type", "Wetland Types", "1.0", "Global", "Ramsar Convention")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WT", "Wetland Types", 1, None),
    ("WT.01", "Freshwater Marsh", 2, 'WT'),
    ("WT.02", "Saltwater Marsh", 2, 'WT'),
    ("WT.03", "Swamp (Forested Wetland)", 2, 'WT'),
    ("WT.04", "Bog (Peat Wetland)", 2, 'WT'),
    ("WT.05", "Fen", 2, 'WT'),
    ("WT.06", "Mangrove Wetland", 2, 'WT'),
    ("WT.07", "Tidal Flat", 2, 'WT'),
    ("WT.08", "Vernal Pool", 2, 'WT'),
    ("WT.09", "Riparian Wetland", 2, 'WT'),
    ("WT.10", "Constructed Wetland", 2, 'WT'),
    ("WT.11", "Prairie Pothole", 2, 'WT'),
    ("WT.12", "Floodplain Wetland", 2, 'WT'),
    ("WT.13", "Wetland Delineation Method", 2, 'WT'),
]

async def ingest_domain_wetland(conn) -> int:
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
