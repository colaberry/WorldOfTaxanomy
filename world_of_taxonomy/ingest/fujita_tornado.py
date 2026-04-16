"""Ingest Enhanced Fujita Tornado Damage Scale."""
from __future__ import annotations

_SYSTEM_ROW = ("fujita_tornado", "Fujita Tornado Scale", "Enhanced Fujita Tornado Damage Scale", "2007", "United States", "NWS/NOAA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EF", "EF Scale", 1, None),
    ("EF.0", "EF0: Light Damage (65-85 mph)", 2, 'EF'),
    ("EF.1", "EF1: Moderate Damage (86-110 mph)", 2, 'EF'),
    ("EF.2", "EF2: Considerable Damage (111-135 mph)", 2, 'EF'),
    ("EF.3", "EF3: Severe Damage (136-165 mph)", 2, 'EF'),
    ("EF.4", "EF4: Devastating Damage (166-200 mph)", 2, 'EF'),
    ("EF.5", "EF5: Incredible Damage (200+ mph)", 2, 'EF'),
    ("EF.DI", "Damage Indicators (28 types)", 2, 'EF'),
    ("EF.DOD", "Degrees of Damage", 2, 'EF'),
    ("EF.UB", "Upper/Lower Bound Wind Speeds", 2, 'EF'),
]

async def ingest_fujita_tornado(conn) -> int:
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
