"""Ingest Student Assessment Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_student_assess", "Student Assessment", "Student Assessment Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SA", "Student Assessment Types", 1, None),
    ("SA.01", "Formative Assessment", 2, 'SA'),
    ("SA.02", "Summative Assessment", 2, 'SA'),
    ("SA.03", "Diagnostic Assessment", 2, 'SA'),
    ("SA.04", "Standardized Test", 2, 'SA'),
    ("SA.05", "Portfolio Assessment", 2, 'SA'),
    ("SA.06", "Performance Assessment", 2, 'SA'),
    ("SA.07", "Rubric-Based Assessment", 2, 'SA'),
    ("SA.08", "Peer Assessment", 2, 'SA'),
    ("SA.09", "Self-Assessment", 2, 'SA'),
    ("SA.10", "Computer Adaptive Testing", 2, 'SA'),
    ("SA.11", "Authentic Assessment", 2, 'SA'),
    ("SA.12", "Competency-Based Assessment", 2, 'SA'),
    ("SA.13", "Norm-Referenced Test", 2, 'SA'),
    ("SA.14", "Criterion-Referenced Test", 2, 'SA'),
]

async def ingest_domain_student_assess(conn) -> int:
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
