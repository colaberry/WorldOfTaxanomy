"""Ingest CMS Medicare Star Rating System."""
from __future__ import annotations

_SYSTEM_ROW = ("cms_star", "CMS Star Ratings", "CMS Medicare Star Rating System", "2024", "United States", "CMS")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CS", "CMS Star Domains", 1, None),
    ("CS.01", "Staying Healthy: Screenings", 2, 'CS'),
    ("CS.02", "Managing Chronic Conditions", 2, 'CS'),
    ("CS.03", "Member Experience (CAHPS)", 2, 'CS'),
    ("CS.04", "Member Complaints", 2, 'CS'),
    ("CS.05", "Health Plan Customer Service", 2, 'CS'),
    ("CS.06", "Drug Plan Customer Service", 2, 'CS'),
    ("CS.07", "Drug Safety and Accuracy", 2, 'CS'),
    ("CS.08", "Drug Plan Cost/Coverage", 2, 'CS'),
    ("CS.09", "Improvement Measures", 2, 'CS'),
    ("CS.10", "Overall Star Rating (1-5)", 2, 'CS'),
    ("CS.11", "Part C Summary (1-5)", 2, 'CS'),
    ("CS.12", "Part D Summary (1-5)", 2, 'CS'),
]

async def ingest_cms_star(conn) -> int:
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
