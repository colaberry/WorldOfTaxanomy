"""Ingest Web of Science Research Areas."""
from __future__ import annotations

_SYSTEM_ROW = ("wos_categories", "WoS Categories", "Web of Science Research Areas", "2024", "Global", "Clarivate")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Clarivate License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WoS", "WoS Research Areas", 1, None),
    ("WoS.01", "Life Sciences and Biomedicine", 2, 'WoS'),
    ("WoS.02", "Physical Sciences", 2, 'WoS'),
    ("WoS.03", "Technology", 2, 'WoS'),
    ("WoS.04", "Social Sciences", 2, 'WoS'),
    ("WoS.05", "Arts and Humanities", 2, 'WoS'),
    ("WoS.LS1", "Agriculture", 2, 'WoS'),
    ("WoS.LS2", "Biochemistry and Molecular Biology", 2, 'WoS'),
    ("WoS.LS3", "Ecology", 2, 'WoS'),
    ("WoS.LS4", "Neuroscience", 2, 'WoS'),
    ("WoS.LS5", "Pharmacology", 2, 'WoS'),
    ("WoS.PS1", "Chemistry", 2, 'WoS'),
    ("WoS.PS2", "Geosciences", 2, 'WoS'),
    ("WoS.PS3", "Mathematics", 2, 'WoS'),
    ("WoS.PS4", "Physics", 2, 'WoS'),
    ("WoS.T1", "Computer Science", 2, 'WoS'),
    ("WoS.T2", "Engineering", 2, 'WoS'),
    ("WoS.T3", "Materials Science", 2, 'WoS'),
    ("WoS.SS1", "Business and Economics", 2, 'WoS'),
    ("WoS.SS2", "Education", 2, 'WoS'),
    ("WoS.SS3", "Law", 2, 'WoS'),
    ("WoS.SS4", "Psychology", 2, 'WoS'),
    ("WoS.AH1", "History", 2, 'WoS'),
    ("WoS.AH2", "Literature", 2, 'WoS'),
    ("WoS.AH3", "Philosophy", 2, 'WoS'),
]

async def ingest_wos_categories(conn) -> int:
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
