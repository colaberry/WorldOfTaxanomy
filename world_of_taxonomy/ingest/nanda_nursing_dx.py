"""Ingest NANDA International Nursing Diagnosis Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("nanda_nursing_dx", "NANDA-I", "NANDA International Nursing Diagnosis Categories", "2024", "Global", "NANDA International")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ND", "NANDA-I Domains", 1, None),
    ("ND.01", "Health Promotion", 2, 'ND'),
    ("ND.02", "Nutrition", 2, 'ND'),
    ("ND.03", "Elimination and Exchange", 2, 'ND'),
    ("ND.04", "Activity/Rest", 2, 'ND'),
    ("ND.05", "Perception/Cognition", 2, 'ND'),
    ("ND.06", "Self-Perception", 2, 'ND'),
    ("ND.07", "Role Relationships", 2, 'ND'),
    ("ND.08", "Sexuality", 2, 'ND'),
    ("ND.09", "Coping/Stress Tolerance", 2, 'ND'),
    ("ND.10", "Life Principles", 2, 'ND'),
    ("ND.11", "Safety/Protection", 2, 'ND'),
    ("ND.12", "Comfort", 2, 'ND'),
    ("ND.13", "Growth/Development", 2, 'ND'),
]

async def ingest_nanda_nursing_dx(conn) -> int:
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
