"""Ingest Numeric Pain Rating Scale Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("pain_scale", "Pain Scale", "Numeric Pain Rating Scale Categories", "2024", "Global", "IASP")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PS", "Pain Scale", 1, None),
    ("PS.00", "0: No Pain", 2, 'PS'),
    ("PS.01", "1-3: Mild Pain", 2, 'PS'),
    ("PS.04", "4-6: Moderate Pain", 2, 'PS'),
    ("PS.07", "7-9: Severe Pain", 2, 'PS'),
    ("PS.10", "10: Worst Possible Pain", 2, 'PS'),
    ("PS.11", "NRS (Numeric Rating Scale)", 2, 'PS'),
    ("PS.12", "VAS (Visual Analog Scale)", 2, 'PS'),
    ("PS.13", "Wong-Baker FACES Scale", 2, 'PS'),
    ("PS.14", "FLACC Scale (Pediatric)", 2, 'PS'),
    ("PS.15", "McGill Pain Questionnaire", 2, 'PS'),
    ("PS.16", "Brief Pain Inventory (BPI)", 2, 'PS'),
]

async def ingest_pain_scale(conn) -> int:
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
