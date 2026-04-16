"""Ingest African Continental Free Trade Area Tariff."""
from __future__ import annotations

_SYSTEM_ROW = ("afcfta_tariff", "AfCFTA Tariff", "African Continental Free Trade Area Tariff", "2021", "Africa", "African Union")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AF", "AfCFTA Categories", 1, None),
    ("AF.01", "Sensitive Products", 2, 'AF'),
    ("AF.02", "Semi-Sensitive Products", 2, 'AF'),
    ("AF.03", "Non-Sensitive Products", 2, 'AF'),
    ("AF.04", "Excluded Products", 2, 'AF'),
    ("AF.05", "Agricultural Products", 2, 'AF'),
    ("AF.06", "Industrial Goods", 2, 'AF'),
    ("AF.07", "Textiles and Clothing", 2, 'AF'),
    ("AF.08", "Automotive Products", 2, 'AF'),
    ("AF.09", "Pharmaceuticals", 2, 'AF'),
    ("AF.10", "Electronics", 2, 'AF'),
    ("AF.11", "Chemicals", 2, 'AF'),
    ("AF.12", "Mining and Mineral Products", 2, 'AF'),
    ("AF.13", "Petroleum Products", 2, 'AF'),
    ("AF.14", "Services Schedules", 2, 'AF'),
]

async def ingest_afcfta_tariff(conn) -> int:
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
