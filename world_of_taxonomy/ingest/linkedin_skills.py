"""Ingest LinkedIn Skills Taxonomy (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("linkedin_skills", "LinkedIn Skills", "LinkedIn Skills Taxonomy (Skeleton)", "2024", "Global", "LinkedIn")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LS", "LinkedIn Skills Categories", 1, None),
    ("LS.01", "Information Technology", 2, 'LS'),
    ("LS.02", "Business Development", 2, 'LS'),
    ("LS.03", "Marketing and Advertising", 2, 'LS'),
    ("LS.04", "Finance and Accounting", 2, 'LS'),
    ("LS.05", "Healthcare and Medical", 2, 'LS'),
    ("LS.06", "Engineering and Manufacturing", 2, 'LS'),
    ("LS.07", "Education and Training", 2, 'LS'),
    ("LS.08", "Legal and Compliance", 2, 'LS'),
    ("LS.09", "Human Resources", 2, 'LS'),
    ("LS.10", "Design and Creative", 2, 'LS'),
    ("LS.11", "Sales and Retail", 2, 'LS'),
    ("LS.12", "Operations and Logistics", 2, 'LS'),
    ("LS.13", "Data Science and Analytics", 2, 'LS'),
    ("LS.14", "Cybersecurity", 2, 'LS'),
    ("LS.15", "Project Management", 2, 'LS'),
    ("LS.16", "Communication and Writing", 2, 'LS'),
]

async def ingest_linkedin_skills(conn) -> int:
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
