"""Ingest Disaster Recovery Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_dr", "Disaster Recovery", "Disaster Recovery Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DR", "Disaster Recovery Types", 1, None),
    ("DR.01", "Hot Site", 2, 'DR'),
    ("DR.02", "Warm Site", 2, 'DR'),
    ("DR.03", "Cold Site", 2, 'DR'),
    ("DR.04", "Cloud-Based DR (DRaaS)", 2, 'DR'),
    ("DR.05", "Active-Active Configuration", 2, 'DR'),
    ("DR.06", "Active-Passive Configuration", 2, 'DR'),
    ("DR.07", "Pilot Light", 2, 'DR'),
    ("DR.08", "RPO/RTO Definition", 2, 'DR'),
    ("DR.09", "Data Replication (Synchronous)", 2, 'DR'),
    ("DR.10", "Data Replication (Asynchronous)", 2, 'DR'),
    ("DR.11", "Failover Testing", 2, 'DR'),
    ("DR.12", "DR Runbook", 2, 'DR'),
    ("DR.13", "Geographic Redundancy", 2, 'DR'),
    ("DR.14", "DR Compliance Audit", 2, 'DR'),
]

async def ingest_domain_dr(conn) -> int:
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
