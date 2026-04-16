"""Ingest Carbon Offset Protocol Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_carbon_offset", "Carbon Offset Protocol", "Carbon Offset Protocol Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CP", "Carbon Offset Protocol Types", 1, None),
    ("CP.01", "Verified Carbon Standard (VCS)", 2, 'CP'),
    ("CP.02", "Gold Standard", 2, 'CP'),
    ("CP.03", "CDM (Clean Development)", 2, 'CP'),
    ("CP.04", "American Carbon Registry", 2, 'CP'),
    ("CP.05", "Climate Action Reserve", 2, 'CP'),
    ("CP.06", "Plan Vivo", 2, 'CP'),
    ("CP.07", "REDD+ Forest Carbon", 2, 'CP'),
    ("CP.08", "Afforestation/Reforestation", 2, 'CP'),
    ("CP.09", "Soil Carbon Sequestration", 2, 'CP'),
    ("CP.10", "Blue Carbon (Ocean)", 2, 'CP'),
    ("CP.11", "Direct Air Capture Credit", 2, 'CP'),
    ("CP.12", "Biochar Carbon Removal", 2, 'CP'),
    ("CP.13", "Article 6 (Paris Agreement)", 2, 'CP'),
]

async def ingest_domain_carbon_offset(conn) -> int:
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
