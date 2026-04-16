"""Ingest Unicode Emoji Category Groups."""
from __future__ import annotations

_SYSTEM_ROW = ("emoji_categories", "Emoji Categories", "Unicode Emoji Category Groups", "16.0", "Global", "Unicode Consortium")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Unicode License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EM", "Emoji Categories", 1, None),
    ("EM.01", "Smileys and Emotion", 2, 'EM'),
    ("EM.02", "People and Body", 2, 'EM'),
    ("EM.03", "Animals and Nature", 2, 'EM'),
    ("EM.04", "Food and Drink", 2, 'EM'),
    ("EM.05", "Travel and Places", 2, 'EM'),
    ("EM.06", "Activities", 2, 'EM'),
    ("EM.07", "Objects", 2, 'EM'),
    ("EM.08", "Symbols", 2, 'EM'),
    ("EM.09", "Flags", 2, 'EM'),
    ("EM.10", "Component (Skin Tones/Hair)", 2, 'EM'),
    ("EM.11", "Emoji Sequences (ZWJ)", 2, 'EM'),
    ("EM.12", "Emoji Modifiers", 2, 'EM'),
]

async def ingest_emoji_categories(conn) -> int:
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
