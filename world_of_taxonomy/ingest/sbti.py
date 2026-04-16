"""Ingest Science Based Targets Initiative Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("sbti", "SBTi", "Science Based Targets Initiative Categories", "2024", "Global", "SBTi")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY-SA 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SB", "SBTi Categories", 1, None),
    ("SB.01", "Near-Term Targets (5-10 years)", 2, 'SB'),
    ("SB.02", "Long-Term Targets (by 2050)", 2, 'SB'),
    ("SB.03", "Net-Zero Standard", 2, 'SB'),
    ("SB.04", "Scope 1 Targets", 2, 'SB'),
    ("SB.05", "Scope 2 Targets", 2, 'SB'),
    ("SB.06", "Scope 3 Targets", 2, 'SB'),
    ("SB.07", "FLAG (Forest, Land, Agriculture)", 2, 'SB'),
    ("SB.08", "Financial Institutions", 2, 'SB'),
    ("SB.09", "SME Route", 2, 'SB'),
    ("SB.10", "1.5C Aligned Pathway", 2, 'SB'),
    ("SB.11", "Well-Below 2C Pathway", 2, 'SB'),
    ("SB.12", "Sector-Specific Guidance", 2, 'SB'),
    ("SB.13", "Verification and Monitoring", 2, 'SB'),
]

async def ingest_sbti(conn) -> int:
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
