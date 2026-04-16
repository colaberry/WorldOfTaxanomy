"""Ingest Australian Qualifications Framework."""
from __future__ import annotations

_SYSTEM_ROW = ("aqf", "AQF", "Australian Qualifications Framework", "2013", "Australia", "Australian Government")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AQF", "AQF Levels", 1, None),
    ("AQF.1", "Level 1 - Senior Secondary Certificate", 2, 'AQF'),
    ("AQF.2", "Level 2 - Certificate I", 2, 'AQF'),
    ("AQF.3", "Level 3 - Certificate II", 2, 'AQF'),
    ("AQF.4", "Level 4 - Certificate III", 2, 'AQF'),
    ("AQF.5", "Level 5 - Certificate IV / Diploma", 2, 'AQF'),
    ("AQF.6", "Level 6 - Advanced Diploma / Associate Degree", 2, 'AQF'),
    ("AQF.7", "Level 7 - Bachelor Degree", 2, 'AQF'),
    ("AQF.8", "Level 8 - Bachelor Honours / Grad Cert / Grad Dip", 2, 'AQF'),
    ("AQF.9", "Level 9 - Masters Degree", 2, 'AQF'),
    ("AQF.10", "Level 10 - Doctoral Degree", 2, 'AQF'),
    ("AQF.K", "Knowledge Dimension", 2, 'AQF'),
    ("AQF.S", "Skills Dimension", 2, 'AQF'),
    ("AQF.A", "Application Dimension", 2, 'AQF'),
]

async def ingest_aqf(conn) -> int:
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
