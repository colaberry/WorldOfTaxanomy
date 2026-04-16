"""Ingest Global Budget Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_global_budget", "Global Budget", "Global Budget Types", "1.0", "United States", "CMS")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GB", "Global Budget Types", 1, None),
    ("GB.01", "Hospital Global Budget", 2, 'GB'),
    ("GB.02", "System-Level Global Budget", 2, 'GB'),
    ("GB.03", "Population-Based Budget", 2, 'GB'),
    ("GB.04", "Maryland All-Payer Model", 2, 'GB'),
    ("GB.05", "Pennsylvania Rural Health Model", 2, 'GB'),
    ("GB.06", "Vermont All-Payer ACO", 2, 'GB'),
    ("GB.07", "Fixed Annual Revenue", 2, 'GB'),
    ("GB.08", "Revenue Growth Limit", 2, 'GB'),
    ("GB.09", "Quality Adjustment", 2, 'GB'),
    ("GB.10", "Volume Adjustment", 2, 'GB'),
    ("GB.11", "Outlier Protection", 2, 'GB'),
    ("GB.12", "Performance Corridor", 2, 'GB'),
    ("GB.13", "Budget Reconciliation", 2, 'GB'),
]

async def ingest_domain_global_budget(conn) -> int:
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
