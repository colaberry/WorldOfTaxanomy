"""Ingest Zero Trust Architecture Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_zero_trust", "Zero Trust Architecture", "Zero Trust Architecture Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ZT", "Zero Trust Architecture Types", 1, None),
    ("ZT.01", "Identity-Centric Security", 2, 'ZT'),
    ("ZT.02", "Micro-Segmentation", 2, 'ZT'),
    ("ZT.03", "Software-Defined Perimeter", 2, 'ZT'),
    ("ZT.04", "Continuous Verification", 2, 'ZT'),
    ("ZT.05", "Least Privilege Access", 2, 'ZT'),
    ("ZT.06", "Device Trust Evaluation", 2, 'ZT'),
    ("ZT.07", "Network Access Control", 2, 'ZT'),
    ("ZT.08", "Application Access Broker", 2, 'ZT'),
    ("ZT.09", "Data-Centric Security", 2, 'ZT'),
    ("ZT.10", "Conditional Access Policy", 2, 'ZT'),
    ("ZT.11", "Session Trust Scoring", 2, 'ZT'),
    ("ZT.12", "Zero Trust Network Access (ZTNA)", 2, 'ZT'),
    ("ZT.13", "Policy Engine", 2, 'ZT'),
    ("ZT.14", "Policy Decision Point", 2, 'ZT'),
    ("ZT.15", "Policy Enforcement Point", 2, 'ZT'),
]

async def ingest_domain_zero_trust(conn) -> int:
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
