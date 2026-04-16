"""Ingest PRINCE2 Process and Theme Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("prince2", "PRINCE2", "PRINCE2 Process and Theme Categories", "7", "Global", "PeopleCert/Axelos")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("P2", "PRINCE2 Categories", 1, None),
    ("P2.01", "Starting Up a Project (SU)", 2, 'P2'),
    ("P2.02", "Directing a Project (DP)", 2, 'P2'),
    ("P2.03", "Initiating a Project (IP)", 2, 'P2'),
    ("P2.04", "Controlling a Stage (CS)", 2, 'P2'),
    ("P2.05", "Managing Product Delivery (MP)", 2, 'P2'),
    ("P2.06", "Managing a Stage Boundary (SB)", 2, 'P2'),
    ("P2.07", "Closing a Project (CP)", 2, 'P2'),
    ("P2.08", "Business Case Theme", 2, 'P2'),
    ("P2.09", "Organization Theme", 2, 'P2'),
    ("P2.10", "Quality Theme", 2, 'P2'),
    ("P2.11", "Plans Theme", 2, 'P2'),
    ("P2.12", "Risk Theme", 2, 'P2'),
    ("P2.13", "Change Theme", 2, 'P2'),
    ("P2.14", "Progress Theme", 2, 'P2'),
]

async def ingest_prince2(conn) -> int:
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
