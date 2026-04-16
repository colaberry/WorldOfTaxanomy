"""Ingest SHRM Body of Applied Skills and Knowledge."""
from __future__ import annotations

_SYSTEM_ROW = ("shrm_competency", "SHRM Competency", "SHRM Body of Applied Skills and Knowledge", "2024", "United States", "SHRM")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SH", "SHRM Competencies", 1, None),
    ("SH.01", "Leadership and Navigation", 2, 'SH'),
    ("SH.02", "Ethical Practice", 2, 'SH'),
    ("SH.03", "Business Acumen", 2, 'SH'),
    ("SH.04", "Relationship Management", 2, 'SH'),
    ("SH.05", "Consultation", 2, 'SH'),
    ("SH.06", "Critical Evaluation", 2, 'SH'),
    ("SH.07", "Global and Cultural Effectiveness", 2, 'SH'),
    ("SH.08", "Communication", 2, 'SH'),
    ("SH.09", "People (HR Expertise)", 2, 'SH'),
    ("SH.10", "Organization (HR Expertise)", 2, 'SH'),
    ("SH.11", "Workplace (HR Expertise)", 2, 'SH'),
    ("SH.12", "Talent Acquisition", 2, 'SH'),
    ("SH.13", "Employee Engagement", 2, 'SH'),
    ("SH.14", "Learning and Development", 2, 'SH'),
    ("SH.15", "Total Rewards", 2, 'SH'),
]

async def ingest_shrm_competency(conn) -> int:
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
