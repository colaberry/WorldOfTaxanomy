"""Ingest NATO Standardization Agreement Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("stanag", "STANAG Categories", "NATO Standardization Agreement Categories", "2024", "Global", "NATO NSO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SG", "STANAG Areas", 1, None),
    ("SG.01", "Operations and Exercises", 2, 'SG'),
    ("SG.02", "Intelligence", 2, 'SG'),
    ("SG.03", "Personnel", 2, 'SG'),
    ("SG.04", "Logistics", 2, 'SG'),
    ("SG.05", "Communications and Information Systems", 2, 'SG'),
    ("SG.06", "Nuclear, Biological, Chemical Defence", 2, 'SG'),
    ("SG.07", "Materiel Standardization", 2, 'SG'),
    ("SG.08", "Medical", 2, 'SG'),
    ("SG.09", "Training", 2, 'SG'),
    ("SG.10", "Land Operations", 2, 'SG'),
    ("SG.11", "Maritime Operations", 2, 'SG'),
    ("SG.12", "Air Operations", 2, 'SG'),
    ("SG.13", "Joint Operations", 2, 'SG'),
    ("SG.14", "Cyber Defence", 2, 'SG'),
    ("SG.15", "Space Operations", 2, 'SG'),
]

async def ingest_stanag(conn) -> int:
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
