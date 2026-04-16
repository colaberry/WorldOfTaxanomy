"""Ingest FIFA Continental Football Confederations."""
from __future__ import annotations

_SYSTEM_ROW = ("fifa_confederations", "FIFA Confederations", "FIFA Continental Football Confederations", "2024", "Global", "FIFA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FC", "FIFA Confederations", 1, None),
    ("FC.01", "AFC (Asia)", 2, 'FC'),
    ("FC.02", "CAF (Africa)", 2, 'FC'),
    ("FC.03", "CONCACAF (North/Central America, Caribbean)", 2, 'FC'),
    ("FC.04", "CONMEBOL (South America)", 2, 'FC'),
    ("FC.05", "OFC (Oceania)", 2, 'FC'),
    ("FC.06", "UEFA (Europe)", 2, 'FC'),
    ("FC.07", "FIFA World Cup", 2, 'FC'),
    ("FC.08", "FIFA Women's World Cup", 2, 'FC'),
    ("FC.09", "FIFA Club World Cup", 2, 'FC'),
    ("FC.10", "FIFA U-20 World Cup", 2, 'FC'),
    ("FC.11", "FIFA U-17 World Cup", 2, 'FC'),
    ("FC.12", "FIFA Futsal World Cup", 2, 'FC'),
    ("FC.13", "FIFA Beach Soccer World Cup", 2, 'FC'),
]

async def ingest_fifa_confederations(conn) -> int:
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
