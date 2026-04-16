"""Ingest Capacity Market Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_capacity_mkt", "Capacity Market", "Capacity Market Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CK", "Capacity Market Types", 1, None),
    ("CK.01", "Forward Capacity Market", 2, 'CK'),
    ("CK.02", "Reliability Pricing Model", 2, 'CK'),
    ("CK.03", "Capacity Auction", 2, 'CK'),
    ("CK.04", "Strategic Reserve", 2, 'CK'),
    ("CK.05", "Decentralized Reliability Option", 2, 'CK'),
    ("CK.06", "Energy-Only Market", 2, 'CK'),
    ("CK.07", "Capacity Performance Obligation", 2, 'CK'),
    ("CK.08", "Demand-Side Capacity", 2, 'CK'),
    ("CK.09", "Storage Capacity Qualification", 2, 'CK'),
    ("CK.10", "Renewable Capacity Credit", 2, 'CK'),
    ("CK.11", "Capacity Transfer Rights", 2, 'CK'),
    ("CK.12", "Resource Adequacy", 2, 'CK'),
]

async def ingest_domain_capacity_mkt(conn) -> int:
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
