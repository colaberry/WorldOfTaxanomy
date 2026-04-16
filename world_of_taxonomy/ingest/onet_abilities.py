"""Ingest O*NET Abilities."""
from __future__ import annotations

_SYSTEM_ROW = ("onet_abilities", "O*NET Abilities", "O*NET Abilities", "2024", "United States", "DOL")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ONA", "O*NET Abilities", 1, None),
    ("ONA.01", "Cognitive Abilities", 2, 'ONA'),
    ("ONA.02", "Psychomotor Abilities", 2, 'ONA'),
    ("ONA.03", "Physical Abilities", 2, 'ONA'),
    ("ONA.04", "Sensory Abilities", 2, 'ONA'),
    ("ONA.C1", "Verbal Abilities", 2, 'ONA'),
    ("ONA.C2", "Idea Generation and Reasoning", 2, 'ONA'),
    ("ONA.C3", "Quantitative Abilities", 2, 'ONA'),
    ("ONA.C4", "Memory", 2, 'ONA'),
    ("ONA.C5", "Perceptual Abilities", 2, 'ONA'),
    ("ONA.C6", "Spatial Abilities", 2, 'ONA'),
    ("ONA.C7", "Attentiveness", 2, 'ONA'),
    ("ONA.P1", "Fine Manipulative Abilities", 2, 'ONA'),
    ("ONA.P2", "Control Movement Abilities", 2, 'ONA'),
    ("ONA.P3", "Reaction Time and Speed", 2, 'ONA'),
    ("ONA.S1", "Visual Abilities", 2, 'ONA'),
    ("ONA.S2", "Auditory and Speech Abilities", 2, 'ONA'),
]

async def ingest_onet_abilities(conn) -> int:
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
