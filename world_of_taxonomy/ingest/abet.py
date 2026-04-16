"""Ingest ABET Engineering Accreditation Criteria."""
from __future__ import annotations

_SYSTEM_ROW = ("abet", "ABET Criteria", "ABET Engineering Accreditation Criteria", "2024", "Global", "ABET Inc.")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AB", "ABET Criteria", 1, None),
    ("AB.01", "Students", 2, 'AB'),
    ("AB.02", "Program Educational Objectives", 2, 'AB'),
    ("AB.03", "Student Outcomes (1-7)", 2, 'AB'),
    ("AB.04", "Continuous Improvement", 2, 'AB'),
    ("AB.05", "Curriculum", 2, 'AB'),
    ("AB.06", "Faculty", 2, 'AB'),
    ("AB.07", "Facilities", 2, 'AB'),
    ("AB.08", "Institutional Support", 2, 'AB'),
    ("AB.09", "Program Criteria (Discipline)", 2, 'AB'),
    ("AB.10", "General Education Component", 2, 'AB'),
    ("AB.11", "Mathematics and Basic Sciences", 2, 'AB'),
    ("AB.12", "Engineering Topics", 2, 'AB'),
    ("AB.13", "Capstone Design Experience", 2, 'AB'),
]

async def ingest_abet(conn) -> int:
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
