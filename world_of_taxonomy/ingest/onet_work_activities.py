"""Ingest O*NET Work Activity Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("onet_work_activities", "O*NET Work Activities", "O*NET Work Activity Categories", "28.0", "United States", "DOL/O*NET")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WA", "Work Activities", 1, None),
    ("WA.01", "Information Input", 2, 'WA'),
    ("WA.02", "Mental Processes", 2, 'WA'),
    ("WA.03", "Work Output", 2, 'WA'),
    ("WA.04", "Interacting With Others", 2, 'WA'),
    ("WA.05", "Looking at Data", 2, 'WA'),
    ("WA.06", "Identifying Objects/Actions", 2, 'WA'),
    ("WA.07", "Estimating Characteristics", 2, 'WA'),
    ("WA.08", "Judging Quality", 2, 'WA'),
    ("WA.09", "Processing Information", 2, 'WA'),
    ("WA.10", "Evaluating Information", 2, 'WA'),
    ("WA.11", "Making Decisions", 2, 'WA'),
    ("WA.12", "Creative Thinking", 2, 'WA'),
    ("WA.13", "Updating Knowledge", 2, 'WA'),
    ("WA.14", "Scheduling Work", 2, 'WA'),
    ("WA.15", "Organizing and Planning", 2, 'WA'),
]

async def ingest_onet_work_activities(conn) -> int:
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
