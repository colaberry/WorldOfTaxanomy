"""Ingest Standard Transportation Commodity Code."""
from __future__ import annotations

_SYSTEM_ROW = ("stcc", "STCC", "Standard Transportation Commodity Code", "2024", "United States", "AAR")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ST", "STCC Groups", 1, None),
    ("ST.01", "Farm Products", 2, 'ST'),
    ("ST.08", "Forest Products", 2, 'ST'),
    ("ST.10", "Metallic Ores", 2, 'ST'),
    ("ST.11", "Coal", 2, 'ST'),
    ("ST.13", "Crude Petroleum", 2, 'ST'),
    ("ST.14", "Non-Metallic Minerals", 2, 'ST'),
    ("ST.19", "Ordnance or Accessories", 2, 'ST'),
    ("ST.20", "Food or Kindred Products", 2, 'ST'),
    ("ST.24", "Lumber or Wood Products", 2, 'ST'),
    ("ST.26", "Pulp, Paper or Allied Products", 2, 'ST'),
    ("ST.28", "Chemicals or Allied Products", 2, 'ST'),
    ("ST.29", "Petroleum or Coal Products", 2, 'ST'),
    ("ST.32", "Clay, Concrete, Glass, Stone", 2, 'ST'),
    ("ST.33", "Primary Metal Products", 2, 'ST'),
    ("ST.34", "Fabricated Metal Products", 2, 'ST'),
    ("ST.35", "Machinery", 2, 'ST'),
    ("ST.37", "Transportation Equipment", 2, 'ST'),
    ("ST.40", "Waste or Scrap Materials", 2, 'ST'),
    ("ST.41", "Misc. Freight Shipments", 2, 'ST'),
    ("ST.42", "Empty Containers", 2, 'ST'),
    ("ST.43", "Mail or Contract Traffic", 2, 'ST'),
    ("ST.44", "Freight All Kinds (FAK)", 2, 'ST'),
    ("ST.46", "Misc. Mixed Shipments", 2, 'ST'),
    ("ST.48", "Hazardous Materials", 2, 'ST'),
    ("ST.49", "Hazardous Waste", 2, 'ST'),
]

async def ingest_stcc(conn) -> int:
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
