"""Ingest APGAR Newborn Assessment Score."""
from __future__ import annotations

_SYSTEM_ROW = ("apgar_score", "APGAR Score", "APGAR Newborn Assessment Score", "1952", "Global", "Virginia Apgar")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AP", "APGAR Components", 1, None),
    ("AP.01", "Appearance (Skin Color)", 2, 'AP'),
    ("AP.02", "Pulse (Heart Rate)", 2, 'AP'),
    ("AP.03", "Grimace (Reflex Irritability)", 2, 'AP'),
    ("AP.04", "Activity (Muscle Tone)", 2, 'AP'),
    ("AP.05", "Respiration (Breathing)", 2, 'AP'),
    ("AP.06", "Score 0 (Absent/Blue)", 2, 'AP'),
    ("AP.07", "Score 1 (Intermediate)", 2, 'AP'),
    ("AP.08", "Score 2 (Normal/Active)", 2, 'AP'),
    ("AP.09", "Total Score 7-10: Normal", 2, 'AP'),
    ("AP.10", "Total Score 4-6: Moderately Abnormal", 2, 'AP'),
    ("AP.11", "Total Score 0-3: Critically Low", 2, 'AP'),
]

async def ingest_apgar_score(conn) -> int:
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
