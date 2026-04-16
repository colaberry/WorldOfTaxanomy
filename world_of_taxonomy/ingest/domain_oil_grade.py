"""Ingest Oil Grade Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_oil_grade", "Oil Grade", "Oil Grade Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("OI", "Oil Grade Types", 1, None),
    ("OI.01", "WTI (West Texas Intermediate)", 2, 'OI'),
    ("OI.02", "Brent Crude", 2, 'OI'),
    ("OI.03", "Dubai/Oman Crude", 2, 'OI'),
    ("OI.04", "Louisiana Light Sweet", 2, 'OI'),
    ("OI.05", "OPEC Basket", 2, 'OI'),
    ("OI.06", "Urals Crude", 2, 'OI'),
    ("OI.07", "Bonny Light (Nigeria)", 2, 'OI'),
    ("OI.08", "Tapis (Malaysia)", 2, 'OI'),
    ("OI.09", "Canadian Heavy (WCS)", 2, 'OI'),
    ("OI.10", "API Gravity Classification", 2, 'OI'),
    ("OI.11", "Sweet vs Sour Crude", 2, 'OI'),
    ("OI.12", "Condensate", 2, 'OI'),
    ("OI.13", "Synthetic Crude", 2, 'OI'),
]

async def ingest_domain_oil_grade(conn) -> int:
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
