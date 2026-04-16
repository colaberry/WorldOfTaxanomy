"""Ingest Master Data Management Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_master_data", "Master Data", "Master Data Management Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MD", "Master Data Management Types", 1, None),
    ("MD.01", "Customer Master Data", 2, 'MD'),
    ("MD.02", "Product Master Data", 2, 'MD'),
    ("MD.03", "Supplier Master Data", 2, 'MD'),
    ("MD.04", "Employee Master Data", 2, 'MD'),
    ("MD.05", "Location Master Data", 2, 'MD'),
    ("MD.06", "Asset Master Data", 2, 'MD'),
    ("MD.07", "Chart of Accounts Master", 2, 'MD'),
    ("MD.08", "Material Master Data", 2, 'MD'),
    ("MD.09", "Golden Record", 2, 'MD'),
    ("MD.10", "Match and Merge", 2, 'MD'),
    ("MD.11", "Survivorship Rules", 2, 'MD'),
    ("MD.12", "MDM Hub Architecture", 2, 'MD'),
    ("MD.13", "MDM Registry Style", 2, 'MD'),
    ("MD.14", "Data Steward Workflow", 2, 'MD'),
    ("MD.15", "Cross-Domain MDM", 2, 'MD'),
]

async def ingest_domain_master_data(conn) -> int:
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
