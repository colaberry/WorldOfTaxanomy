"""Ingest Universal Decimal Classification (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("udc", "UDC", "Universal Decimal Classification (Skeleton)", "2024", "Global", "UDC Consortium")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY-NC-SA 3.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("UD", "UDC Main Tables", 1, None),
    ("UD.0", "Science and Knowledge", 2, 'UD'),
    ("UD.1", "Philosophy, Psychology", 2, 'UD'),
    ("UD.2", "Religion, Theology", 2, 'UD'),
    ("UD.3", "Social Sciences", 2, 'UD'),
    ("UD.4", "(Vacant)", 2, 'UD'),
    ("UD.5", "Natural Sciences, Mathematics", 2, 'UD'),
    ("UD.6", "Applied Sciences, Technology", 2, 'UD'),
    ("UD.7", "Arts, Recreation, Sport", 2, 'UD'),
    ("UD.8", "Language, Linguistics, Literature", 2, 'UD'),
    ("UD.9", "Geography, History", 2, 'UD'),
]

async def ingest_udc(conn) -> int:
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
