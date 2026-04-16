"""Ingest WCO SAFE Framework of Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("wco_safe", "WCO SAFE", "WCO SAFE Framework of Standards", "2024", "Global", "WCO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WS", "WCO SAFE Pillars", 1, None),
    ("WS.01", "Pillar 1: Customs-to-Customs", 2, 'WS'),
    ("WS.02", "Pillar 2: Customs-to-Business", 2, 'WS'),
    ("WS.03", "Pillar 3: Customs-to-Other Agencies", 2, 'WS'),
    ("WS.04", "AEO (Authorized Economic Operator)", 2, 'WS'),
    ("WS.05", "Advance Cargo Information", 2, 'WS'),
    ("WS.06", "Risk Management", 2, 'WS'),
    ("WS.07", "Non-Intrusive Inspection", 2, 'WS'),
    ("WS.08", "Mutual Recognition", 2, 'WS'),
    ("WS.09", "Data Analytics", 2, 'WS'),
    ("WS.10", "E-Commerce Framework", 2, 'WS'),
    ("WS.11", "Trade Facilitation", 2, 'WS'),
    ("WS.12", "Supply Chain Security", 2, 'WS'),
    ("WS.13", "Single Window", 2, 'WS'),
]

async def ingest_wco_safe(conn) -> int:
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
