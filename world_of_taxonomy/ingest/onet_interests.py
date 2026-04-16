"""Ingest O*NET Interest Profiler Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("onet_interests", "O*NET Interests", "O*NET Interest Profiler Categories", "28.0", "United States", "DOL/O*NET")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("OI", "O*NET Interests (RIASEC)", 1, None),
    ("OI.01", "Realistic", 2, 'OI'),
    ("OI.02", "Investigative", 2, 'OI'),
    ("OI.03", "Artistic", 2, 'OI'),
    ("OI.04", "Social", 2, 'OI'),
    ("OI.05", "Enterprising", 2, 'OI'),
    ("OI.06", "Conventional", 2, 'OI'),
    ("OI.07", "Realistic-Investigative", 2, 'OI'),
    ("OI.08", "Artistic-Social", 2, 'OI'),
    ("OI.09", "Enterprising-Conventional", 2, 'OI'),
    ("OI.10", "Investigative-Artistic", 2, 'OI'),
    ("OI.11", "Social-Enterprising", 2, 'OI'),
    ("OI.12", "Realistic-Conventional", 2, 'OI'),
]

async def ingest_onet_interests(conn) -> int:
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
