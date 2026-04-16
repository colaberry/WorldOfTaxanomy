"""Ingest Library of Congress Subject Headings (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("lcsh", "LCSH", "Library of Congress Subject Headings (Skeleton)", "2024", "Global", "Library of Congress")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LC", "LCSH Broad Topics", 1, None),
    ("LC.A", "General Works", 2, 'LC'),
    ("LC.B", "Philosophy, Psychology, Religion", 2, 'LC'),
    ("LC.C", "Auxiliary Sciences of History", 2, 'LC'),
    ("LC.D", "World History", 2, 'LC'),
    ("LC.E", "History of the Americas (General)", 2, 'LC'),
    ("LC.G", "Geography, Anthropology", 2, 'LC'),
    ("LC.H", "Social Sciences", 2, 'LC'),
    ("LC.J", "Political Science", 2, 'LC'),
    ("LC.K", "Law", 2, 'LC'),
    ("LC.L", "Education", 2, 'LC'),
    ("LC.M", "Music", 2, 'LC'),
    ("LC.N", "Fine Arts", 2, 'LC'),
    ("LC.P", "Language and Literature", 2, 'LC'),
    ("LC.Q", "Science", 2, 'LC'),
    ("LC.R", "Medicine", 2, 'LC'),
    ("LC.S", "Agriculture", 2, 'LC'),
    ("LC.T", "Technology", 2, 'LC'),
    ("LC.U", "Military Science", 2, 'LC'),
    ("LC.Z", "Bibliography, Library Science", 2, 'LC'),
]

async def ingest_lcsh(conn) -> int:
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
