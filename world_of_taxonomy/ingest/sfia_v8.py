"""Ingest SFIA v8 (Skills Framework for IT)."""
from __future__ import annotations

_SYSTEM_ROW = ("sfia_v8", "SFIA v8", "SFIA v8 (Skills Framework for IT)", "8", "Global", "SFIA Foundation")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "SFIA License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SFIA", "SFIA Categories", 1, None),
    ("SFIA.ST", "Strategy and architecture", 2, 'SFIA'),
    ("SFIA.CH", "Change and transformation", 2, 'SFIA'),
    ("SFIA.DV", "Development and implementation", 2, 'SFIA'),
    ("SFIA.DL", "Delivery and operation", 2, 'SFIA'),
    ("SFIA.SK", "Skills and quality", 2, 'SFIA'),
    ("SFIA.RM", "Relationships and engagement", 2, 'SFIA'),
    ("SFIA.L1", "Level 1 - Follow", 2, 'SFIA'),
    ("SFIA.L2", "Level 2 - Assist", 2, 'SFIA'),
    ("SFIA.L3", "Level 3 - Apply", 2, 'SFIA'),
    ("SFIA.L4", "Level 4 - Enable", 2, 'SFIA'),
    ("SFIA.L5", "Level 5 - Ensure, advise", 2, 'SFIA'),
    ("SFIA.L6", "Level 6 - Initiate, influence", 2, 'SFIA'),
    ("SFIA.L7", "Level 7 - Set strategy, inspire", 2, 'SFIA'),
]

async def ingest_sfia_v8(conn) -> int:
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
