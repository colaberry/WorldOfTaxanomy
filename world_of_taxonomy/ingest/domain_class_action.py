"""Ingest Class Action Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_class_action", "Class Action", "Class Action Types", "1.0", "United States", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CL", "Class Action Types", 1, None),
    ("CL.01", "Securities Class Action", 2, 'CL'),
    ("CL.02", "Antitrust Class Action", 2, 'CL'),
    ("CL.03", "Consumer Class Action", 2, 'CL'),
    ("CL.04", "Employment Class Action", 2, 'CL'),
    ("CL.05", "Product Liability Mass Tort", 2, 'CL'),
    ("CL.06", "Environmental Class Action", 2, 'CL'),
    ("CL.07", "Data Breach Class Action", 2, 'CL'),
    ("CL.08", "Civil Rights Class Action", 2, 'CL'),
    ("CL.09", "Class Certification (Rule 23)", 2, 'CL'),
    ("CL.10", "Opt-In vs Opt-Out", 2, 'CL'),
    ("CL.11", "Settlement Class", 2, 'CL'),
    ("CL.12", "MDL Consolidation", 2, 'CL'),
    ("CL.13", "Cy Pres Distribution", 2, 'CL'),
]

async def ingest_domain_class_action(conn) -> int:
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
