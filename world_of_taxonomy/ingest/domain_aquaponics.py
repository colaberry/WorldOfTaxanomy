"""Ingest Aquaponics Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_aquaponics", "Aquaponics", "Aquaponics Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AP", "Aquaponics Types", 1, None),
    ("AP.01", "Media-Based Aquaponics", 2, 'AP'),
    ("AP.02", "Deep Water Culture (DWC)", 2, 'AP'),
    ("AP.03", "Nutrient Film Technique (NFT)", 2, 'AP'),
    ("AP.04", "Hybrid Aquaponics System", 2, 'AP'),
    ("AP.05", "Commercial-Scale Aquaponics", 2, 'AP'),
    ("AP.06", "Backyard Aquaponics", 2, 'AP'),
    ("AP.07", "Decoupled Aquaponics", 2, 'AP'),
    ("AP.08", "Biofloc System", 2, 'AP'),
    ("AP.09", "Vertical Aquaponics", 2, 'AP'),
    ("AP.10", "Fish Species Selection", 2, 'AP'),
    ("AP.11", "Plant Species Selection", 2, 'AP'),
    ("AP.12", "Water Quality Management", 2, 'AP'),
]

async def ingest_domain_aquaponics(conn) -> int:
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
