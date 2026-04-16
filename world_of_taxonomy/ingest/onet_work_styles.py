"""Ingest O*NET Work Style Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("onet_work_styles", "O*NET Work Styles", "O*NET Work Style Categories", "28.0", "United States", "DOL/O*NET")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WS", "Work Styles", 1, None),
    ("WS.01", "Achievement/Effort", 2, 'WS'),
    ("WS.02", "Persistence", 2, 'WS'),
    ("WS.03", "Initiative", 2, 'WS'),
    ("WS.04", "Leadership", 2, 'WS'),
    ("WS.05", "Cooperation", 2, 'WS'),
    ("WS.06", "Concern for Others", 2, 'WS'),
    ("WS.07", "Social Orientation", 2, 'WS'),
    ("WS.08", "Self-Control", 2, 'WS'),
    ("WS.09", "Stress Tolerance", 2, 'WS'),
    ("WS.10", "Adaptability/Flexibility", 2, 'WS'),
    ("WS.11", "Dependability", 2, 'WS'),
    ("WS.12", "Attention to Detail", 2, 'WS'),
    ("WS.13", "Integrity", 2, 'WS'),
    ("WS.14", "Independence", 2, 'WS'),
    ("WS.15", "Innovation", 2, 'WS'),
    ("WS.16", "Analytical Thinking", 2, 'WS'),
]

async def ingest_onet_work_styles(conn) -> int:
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
