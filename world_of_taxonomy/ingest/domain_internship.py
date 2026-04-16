"""Ingest Internship Model Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_internship", "Internship Model", "Internship Model Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IN", "Internship Model Types", 1, None),
    ("IN.01", "Summer Internship", 2, 'IN'),
    ("IN.02", "Co-Op Program", 2, 'IN'),
    ("IN.03", "Virtual Internship", 2, 'IN'),
    ("IN.04", "Micro-Internship", 2, 'IN'),
    ("IN.05", "Externship", 2, 'IN'),
    ("IN.06", "Rotational Internship", 2, 'IN'),
    ("IN.07", "Research Internship", 2, 'IN'),
    ("IN.08", "Government Internship", 2, 'IN'),
    ("IN.09", "Nonprofit Internship", 2, 'IN'),
    ("IN.10", "Startup Internship", 2, 'IN'),
    ("IN.11", "Returnship (Career Re-entry)", 2, 'IN'),
    ("IN.12", "Paid vs Unpaid Compliance", 2, 'IN'),
]

async def ingest_domain_internship(conn) -> int:
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
