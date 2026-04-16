"""Ingest Product Liability Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_product_liab", "Product Liability", "Product Liability Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PB", "Product Liability Types", 1, None),
    ("PB.01", "Manufacturing Defect", 2, 'PB'),
    ("PB.02", "Design Defect", 2, 'PB'),
    ("PB.03", "Failure to Warn", 2, 'PB'),
    ("PB.04", "Strict Liability", 2, 'PB'),
    ("PB.05", "Negligence-Based Liability", 2, 'PB'),
    ("PB.06", "Breach of Warranty", 2, 'PB'),
    ("PB.07", "Class Action (Product)", 2, 'PB'),
    ("PB.08", "MDL (Multi-District Litigation)", 2, 'PB'),
    ("PB.09", "Product Recall Trigger", 2, 'PB'),
    ("PB.10", "CPSC Reporting", 2, 'PB'),
    ("PB.11", "Pharmaceutical Liability", 2, 'PB'),
    ("PB.12", "Automotive Recall", 2, 'PB'),
    ("PB.13", "Software/AI Liability", 2, 'PB'),
]

async def ingest_domain_product_liab(conn) -> int:
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
