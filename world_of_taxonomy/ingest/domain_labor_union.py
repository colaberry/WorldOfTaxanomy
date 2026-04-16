"""Ingest Labor Union Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_labor_union", "Labor Union Type", "Labor Union Types", "1.0", "Global", "ILO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LU", "Labor Union Types", 1, None),
    ("LU.01", "Craft Union", 2, 'LU'),
    ("LU.02", "Industrial Union", 2, 'LU'),
    ("LU.03", "General Union", 2, 'LU'),
    ("LU.04", "White-Collar Union", 2, 'LU'),
    ("LU.05", "Public Sector Union", 2, 'LU'),
    ("LU.06", "Trade Union Federation", 2, 'LU'),
    ("LU.07", "Enterprise Union", 2, 'LU'),
    ("LU.08", "Works Council", 2, 'LU'),
    ("LU.09", "Professional Association (Union)", 2, 'LU'),
    ("LU.10", "Union Density Rate", 2, 'LU'),
    ("LU.11", "Right-to-Work State", 2, 'LU'),
    ("LU.12", "Union Certification Process", 2, 'LU'),
]

async def ingest_domain_labor_union(conn) -> int:
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
