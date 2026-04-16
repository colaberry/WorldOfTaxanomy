"""Ingest Code of Federal Regulations Title Index."""
from __future__ import annotations

_SYSTEM_ROW = ("cfr_titles", "CFR Titles", "Code of Federal Regulations Title Index", "2024", "United States", "GPO/OFR")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CF", "CFR Titles", 1, None),
    ("CF.01", "Title 1: General Provisions", 2, 'CF'),
    ("CF.03", "Title 3: The President", 2, 'CF'),
    ("CF.05", "Title 5: Administrative Personnel", 2, 'CF'),
    ("CF.07", "Title 7: Agriculture", 2, 'CF'),
    ("CF.10", "Title 10: Energy", 2, 'CF'),
    ("CF.12", "Title 12: Banks and Banking", 2, 'CF'),
    ("CF.14", "Title 14: Aeronautics and Space", 2, 'CF'),
    ("CF.17", "Title 17: Commodity/Securities Exchanges", 2, 'CF'),
    ("CF.21", "Title 21: Food and Drugs", 2, 'CF'),
    ("CF.26", "Title 26: Internal Revenue", 2, 'CF'),
    ("CF.29", "Title 29: Labor", 2, 'CF'),
    ("CF.33", "Title 33: Navigation and Navigable Waters", 2, 'CF'),
    ("CF.40", "Title 40: Protection of Environment", 2, 'CF'),
    ("CF.42", "Title 42: Public Health", 2, 'CF'),
    ("CF.47", "Title 47: Telecommunication", 2, 'CF'),
    ("CF.48", "Title 48: Federal Acquisition Regulations", 2, 'CF'),
    ("CF.49", "Title 49: Transportation", 2, 'CF'),
    ("CF.50", "Title 50: Wildlife and Fisheries", 2, 'CF'),
]

async def ingest_cfr_titles(conn) -> int:
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
