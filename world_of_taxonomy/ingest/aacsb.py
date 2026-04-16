"""Ingest AACSB Business School Accreditation Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("aacsb", "AACSB Standards", "AACSB Business School Accreditation Standards", "2020", "Global", "AACSB International")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AA", "AACSB Standards", 1, None),
    ("AA.01", "Strategic Management and Innovation", 2, 'AA'),
    ("AA.02", "Learner Success", 2, 'AA'),
    ("AA.03", "Thought Leadership", 2, 'AA'),
    ("AA.04", "Engagement and Societal Impact", 2, 'AA'),
    ("AA.05", "Faculty Qualifications", 2, 'AA'),
    ("AA.06", "Scholarly Academics (SA)", 2, 'AA'),
    ("AA.07", "Practice Academics (PA)", 2, 'AA'),
    ("AA.08", "Scholarly Practitioners (SP)", 2, 'AA'),
    ("AA.09", "Instructional Practitioners (IP)", 2, 'AA'),
    ("AA.10", "Curriculum Management", 2, 'AA'),
    ("AA.11", "Assurance of Learning", 2, 'AA'),
    ("AA.12", "Teaching Effectiveness", 2, 'AA'),
    ("AA.13", "Student/Stakeholder Engagement", 2, 'AA'),
]

async def ingest_aacsb(conn) -> int:
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
