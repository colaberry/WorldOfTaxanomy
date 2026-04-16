"""Ingest Freelance Platform Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_freelance_plat", "Freelance Platform", "Freelance Platform Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FP", "Freelance Platform Types", 1, None),
    ("FP.01", "General Freelance (Upwork)", 2, 'FP'),
    ("FP.02", "Creative Freelance (Fiverr)", 2, 'FP'),
    ("FP.03", "Tech Freelance (Toptal)", 2, 'FP'),
    ("FP.04", "Writing Freelance", 2, 'FP'),
    ("FP.05", "Design Freelance (99designs)", 2, 'FP'),
    ("FP.06", "Consulting Platform", 2, 'FP'),
    ("FP.07", "Legal Freelance", 2, 'FP'),
    ("FP.08", "Translation Platform", 2, 'FP'),
    ("FP.09", "Video/Audio Freelance", 2, 'FP'),
    ("FP.10", "Data Annotation Platform", 2, 'FP'),
    ("FP.11", "Staffing Marketplace", 2, 'FP'),
    ("FP.12", "Talent Cloud Platform", 2, 'FP'),
    ("FP.13", "Project-Based Marketplace", 2, 'FP'),
]

async def ingest_domain_freelance_plat(conn) -> int:
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
