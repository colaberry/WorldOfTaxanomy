"""Ingest ERA Fields of Research (Australia)."""
from __future__ import annotations

_SYSTEM_ROW = ("era_for", "ERA FoR", "ERA Fields of Research (Australia)", "2020", "Australia", "ARC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EF", "ERA FoR Divisions", 1, None),
    ("EF.30", "Agricultural, Veterinary, Food Sciences", 2, 'EF'),
    ("EF.31", "Biological Sciences", 2, 'EF'),
    ("EF.32", "Biomedical and Clinical Sciences", 2, 'EF'),
    ("EF.33", "Built Environment and Design", 2, 'EF'),
    ("EF.34", "Chemical Sciences", 2, 'EF'),
    ("EF.35", "Commerce, Management, Tourism", 2, 'EF'),
    ("EF.36", "Creative Arts and Writing", 2, 'EF'),
    ("EF.37", "Earth Sciences", 2, 'EF'),
    ("EF.38", "Economics", 2, 'EF'),
    ("EF.39", "Education", 2, 'EF'),
    ("EF.40", "Engineering", 2, 'EF'),
    ("EF.41", "Environmental Sciences", 2, 'EF'),
    ("EF.42", "Health Sciences", 2, 'EF'),
    ("EF.43", "History, Heritage, Archaeology", 2, 'EF'),
    ("EF.44", "Human Society", 2, 'EF'),
    ("EF.45", "Indigenous Studies", 2, 'EF'),
    ("EF.46", "Information and Computing Sciences", 2, 'EF'),
    ("EF.47", "Language, Communication, Culture", 2, 'EF'),
    ("EF.48", "Law and Legal Studies", 2, 'EF'),
    ("EF.49", "Mathematical Sciences", 2, 'EF'),
    ("EF.50", "Philosophy and Religious Studies", 2, 'EF'),
    ("EF.51", "Physical Sciences", 2, 'EF'),
    ("EF.52", "Psychology", 2, 'EF'),
]

async def ingest_era_for(conn) -> int:
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
