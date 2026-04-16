"""Ingest ICHI (Health Interventions)."""
from __future__ import annotations

_SYSTEM_ROW = ("ichi_who", "ICHI", "ICHI (Health Interventions)", "2024", "Global", "WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ICHI", "ICHI Axes", 1, None),
    ("ICHI.T", "Target (Anatomy/Function)", 2, 'ICHI'),
    ("ICHI.A", "Action (Intervention Type)", 2, 'ICHI'),
    ("ICHI.M", "Means (Technology/Approach)", 2, 'ICHI'),
    ("ICHI.A1", "Investigation Actions", 2, 'ICHI'),
    ("ICHI.A2", "Treating Actions", 2, 'ICHI'),
    ("ICHI.A3", "Managing Actions", 2, 'ICHI'),
    ("ICHI.A4", "Preventing Actions", 2, 'ICHI'),
    ("ICHI.M1", "Physical/Mechanical Means", 2, 'ICHI'),
    ("ICHI.M2", "Pharmacological Means", 2, 'ICHI'),
    ("ICHI.M3", "Radiation Means", 2, 'ICHI'),
    ("ICHI.M4", "Psychosocial Means", 2, 'ICHI'),
    ("ICHI.M5", "Education Means", 2, 'ICHI'),
    ("ICHI.M6", "Environmental Means", 2, 'ICHI'),
    ("ICHI.EX", "Extensions", 2, 'ICHI'),
]

async def ingest_ichi_who(conn) -> int:
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
