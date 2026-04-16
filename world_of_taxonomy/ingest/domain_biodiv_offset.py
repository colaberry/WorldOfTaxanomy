"""Ingest Biodiversity Offset Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_biodiv_offset", "Biodiversity Offset", "Biodiversity Offset Types", "1.0", "Global", "IUCN")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BO", "Biodiversity Offset Types", 1, None),
    ("BO.01", "Habitat Banking", 2, 'BO'),
    ("BO.02", "Species Banking", 2, 'BO'),
    ("BO.03", "Mitigation Banking (Wetland)", 2, 'BO'),
    ("BO.04", "Conservation Credit", 2, 'BO'),
    ("BO.05", "Biodiversity Net Gain", 2, 'BO'),
    ("BO.06", "No Net Loss Policy", 2, 'BO'),
    ("BO.07", "Offset Equivalence Metric", 2, 'BO'),
    ("BO.08", "In-Kind Offset", 2, 'BO'),
    ("BO.09", "Out-of-Kind Offset", 2, 'BO'),
    ("BO.10", "Offset Additionality", 2, 'BO'),
    ("BO.11", "Offset Permanence", 2, 'BO'),
    ("BO.12", "Offset Monitoring", 2, 'BO'),
]

async def ingest_domain_biodiv_offset(conn) -> int:
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
