"""Ingest European Qualifications Framework."""
from __future__ import annotations

_SYSTEM_ROW = ("eqf", "EQF", "European Qualifications Framework", "2017", "European Union", "European Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EQF", "EQF Levels", 1, None),
    ("EQF.1", "Level 1 - Basic general knowledge", 2, 'EQF'),
    ("EQF.2", "Level 2 - Basic factual knowledge", 2, 'EQF'),
    ("EQF.3", "Level 3 - Knowledge of facts and principles", 2, 'EQF'),
    ("EQF.4", "Level 4 - Factual and theoretical", 2, 'EQF'),
    ("EQF.5", "Level 5 - Comprehensive, specialized", 2, 'EQF'),
    ("EQF.6", "Level 6 - Advanced knowledge (Bachelor)", 2, 'EQF'),
    ("EQF.7", "Level 7 - Highly specialised (Master)", 2, 'EQF'),
    ("EQF.8", "Level 8 - Frontier knowledge (Doctoral)", 2, 'EQF'),
    ("EQF.K", "Knowledge Descriptor", 2, 'EQF'),
    ("EQF.S", "Skills Descriptor", 2, 'EQF'),
    ("EQF.C", "Competence Descriptor", 2, 'EQF'),
    ("EQF.V", "Validation and Recognition", 2, 'EQF'),
]

async def ingest_eqf(conn) -> int:
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
