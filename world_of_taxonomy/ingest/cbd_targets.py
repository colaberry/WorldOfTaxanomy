"""Ingest Convention on Biological Diversity Global Biodiversity Framework Targets."""
from __future__ import annotations

_SYSTEM_ROW = ("cbd_targets", "CBD Targets", "Convention on Biological Diversity Global Biodiversity Framework Targets", "2022", "Global", "CBD Secretariat")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CB", "CBD GBF Targets", 1, None),
    ("CB.01", "Target 1: Spatial Planning", 2, 'CB'),
    ("CB.02", "Target 2: Restoration", 2, 'CB'),
    ("CB.03", "Target 3: 30x30 Conservation", 2, 'CB'),
    ("CB.04", "Target 4: Halt Species Extinction", 2, 'CB'),
    ("CB.05", "Target 5: Sustainable Harvesting", 2, 'CB'),
    ("CB.06", "Target 6: Invasive Alien Species", 2, 'CB'),
    ("CB.07", "Target 7: Reduce Pollution", 2, 'CB'),
    ("CB.08", "Target 8: Climate Change Impacts", 2, 'CB'),
    ("CB.09", "Target 9: Sustainable Use", 2, 'CB'),
    ("CB.10", "Target 10: Agriculture/Forestry Biodiversity", 2, 'CB'),
    ("CB.11", "Target 11: Nature-Based Solutions", 2, 'CB'),
    ("CB.12", "Target 12: Urban Biodiversity", 2, 'CB'),
    ("CB.13", "Target 13: Access and Benefit Sharing", 2, 'CB'),
    ("CB.14", "Target 14: Policy Integration", 2, 'CB'),
    ("CB.15", "Target 15: Business Reporting", 2, 'CB'),
    ("CB.16", "Target 16: Sustainable Consumption", 2, 'CB'),
    ("CB.17", "Target 17: Biosafety", 2, 'CB'),
    ("CB.18", "Target 18: Harmful Subsidies", 2, 'CB'),
    ("CB.19", "Target 19: Resource Mobilization", 2, 'CB'),
    ("CB.20", "Target 20: Capacity Building", 2, 'CB'),
    ("CB.21", "Target 21: Knowledge Management", 2, 'CB'),
    ("CB.22", "Target 22: Indigenous Peoples", 2, 'CB'),
    ("CB.23", "Target 23: Gender Equality", 2, 'CB'),
]

async def ingest_cbd_targets(conn) -> int:
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
