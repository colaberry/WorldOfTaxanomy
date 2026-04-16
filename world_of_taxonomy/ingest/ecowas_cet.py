"""Ingest ECOWAS Common External Tariff."""
from __future__ import annotations

_SYSTEM_ROW = ("ecowas_cet", "ECOWAS CET", "ECOWAS Common External Tariff", "2015", "West Africa", "ECOWAS Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EC", "ECOWAS CET Bands", 1, None),
    ("EC.01", "Band 0 (0%) - Essential Social Goods", 2, 'EC'),
    ("EC.02", "Band 1 (5%) - Essential Goods", 2, 'EC'),
    ("EC.03", "Band 2 (10%) - Intermediate Goods", 2, 'EC'),
    ("EC.04", "Band 3 (20%) - Final Consumption Goods", 2, 'EC'),
    ("EC.05", "Band 4 (35%) - Specific Goods", 2, 'EC'),
    ("EC.06", "Agricultural Products", 2, 'EC'),
    ("EC.07", "Industrial Raw Materials", 2, 'EC'),
    ("EC.08", "Capital Goods", 2, 'EC'),
    ("EC.09", "Consumer Electronics", 2, 'EC'),
    ("EC.10", "Textiles and Apparel", 2, 'EC'),
    ("EC.11", "Automotive", 2, 'EC'),
    ("EC.12", "Pharmaceuticals", 2, 'EC'),
    ("EC.13", "Processed Foods", 2, 'EC'),
]

async def ingest_ecowas_cet(conn) -> int:
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
