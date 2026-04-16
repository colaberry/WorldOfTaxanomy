"""Ingest UNESCO Thesaurus (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("unesco_thesaurus", "UNESCO Thesaurus", "UNESCO Thesaurus (Skeleton)", "2024", "Global", "UNESCO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY-SA 3.0 IGO"

NODES: list[tuple[str, str, int, str | None]] = [
    ("UT", "UNESCO Domains", 1, None),
    ("UT.01", "Education", 2, 'UT'),
    ("UT.02", "Science", 2, 'UT'),
    ("UT.03", "Culture", 2, 'UT'),
    ("UT.04", "Social and Human Sciences", 2, 'UT'),
    ("UT.05", "Information and Communication", 2, 'UT'),
    ("UT.06", "Politics, Law, Economics", 2, 'UT'),
    ("UT.07", "Countries and Country Groupings", 2, 'UT'),
    ("UT.08", "Natural Sciences and Earth Sciences", 2, 'UT'),
    ("UT.09", "Engineering and Technology", 2, 'UT'),
    ("UT.10", "Environmental Sciences", 2, 'UT'),
    ("UT.11", "Health and Nutrition", 2, 'UT'),
    ("UT.12", "Agriculture and Food", 2, 'UT'),
    ("UT.13", "Urban and Rural Development", 2, 'UT'),
    ("UT.14", "Demographics and Population", 2, 'UT'),
]

async def ingest_unesco_thesaurus(conn) -> int:
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
