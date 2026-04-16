"""Ingest International Standard Recording Code Components."""
from __future__ import annotations

_SYSTEM_ROW = ("isrc_format", "ISRC", "International Standard Recording Code Components", "2024", "Global", "IFPI")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IR", "ISRC Components", 1, None),
    ("IR.01", "Country Code (2 chars)", 2, 'IR'),
    ("IR.02", "Registrant Code (3 chars)", 2, 'IR'),
    ("IR.03", "Year of Reference (2 digits)", 2, 'IR'),
    ("IR.04", "Designation Code (5 digits)", 2, 'IR'),
    ("IR.05", "Sound Recording", 2, 'IR'),
    ("IR.06", "Music Video", 2, 'IR'),
    ("IR.07", "Spoken Word", 2, 'IR'),
    ("IR.08", "Podcast Episode", 2, 'IR'),
    ("IR.09", "Audio Drama", 2, 'IR'),
    ("IR.10", "Radio Broadcast Recording", 2, 'IR'),
    ("IR.11", "Live Performance Recording", 2, 'IR'),
    ("IR.12", "Remix/Adaptation", 2, 'IR'),
]

async def ingest_isrc_format(conn) -> int:
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
