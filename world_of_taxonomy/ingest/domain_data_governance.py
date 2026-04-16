"""Ingest Data Governance Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_data_governance", "Data Governance", "Data Governance Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DG", "Data Governance Types", 1, None),
    ("DG.01", "Data Stewardship", 2, 'DG'),
    ("DG.02", "Data Policy Management", 2, 'DG'),
    ("DG.03", "Data Classification Framework", 2, 'DG'),
    ("DG.04", "Data Retention Policy", 2, 'DG'),
    ("DG.05", "Data Access Control", 2, 'DG'),
    ("DG.06", "Data Privacy Compliance", 2, 'DG'),
    ("DG.07", "Data Ethics Framework", 2, 'DG'),
    ("DG.08", "Data Sovereignty", 2, 'DG'),
    ("DG.09", "Cross-Border Data Transfer", 2, 'DG'),
    ("DG.10", "Data Sharing Agreement", 2, 'DG'),
    ("DG.11", "Data Catalog Governance", 2, 'DG'),
    ("DG.12", "Metadata Standard", 2, 'DG'),
    ("DG.13", "Data Governance Council", 2, 'DG'),
    ("DG.14", "Data Maturity Model", 2, 'DG'),
    ("DG.15", "Regulatory Data Reporting", 2, 'DG'),
]

async def ingest_domain_data_governance(conn) -> int:
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
