"""Ingest Generic Job Family Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("job_family", "Job Family Model", "Generic Job Family Classification", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("JF", "Job Families", 1, None),
    ("JF.01", "Executive/C-Suite", 2, 'JF'),
    ("JF.02", "Finance and Accounting", 2, 'JF'),
    ("JF.03", "Human Resources", 2, 'JF'),
    ("JF.04", "Information Technology", 2, 'JF'),
    ("JF.05", "Marketing and Communications", 2, 'JF'),
    ("JF.06", "Sales and Business Development", 2, 'JF'),
    ("JF.07", "Operations and Supply Chain", 2, 'JF'),
    ("JF.08", "Legal and Compliance", 2, 'JF'),
    ("JF.09", "Research and Development", 2, 'JF'),
    ("JF.10", "Engineering", 2, 'JF'),
    ("JF.11", "Customer Service and Support", 2, 'JF'),
    ("JF.12", "Product Management", 2, 'JF'),
    ("JF.13", "Project/Program Management", 2, 'JF'),
    ("JF.14", "Data and Analytics", 2, 'JF'),
    ("JF.15", "Quality Assurance", 2, 'JF'),
    ("JF.16", "Facilities and Administration", 2, 'JF'),
    ("JF.17", "Manufacturing/Production", 2, 'JF'),
    ("JF.18", "Health/Safety/Environment", 2, 'JF'),
]

async def ingest_job_family(conn) -> int:
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
