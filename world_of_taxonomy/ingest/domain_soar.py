"""Ingest SOAR Platform Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_soar", "SOAR Platform", "SOAR Platform Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SO", "SOAR Platform Types", 1, None),
    ("SO.01", "Security Orchestration", 2, 'SO'),
    ("SO.02", "Security Automation", 2, 'SO'),
    ("SO.03", "Incident Response Playbook", 2, 'SO'),
    ("SO.04", "Threat Intelligence Integration", 2, 'SO'),
    ("SO.05", "Case Management", 2, 'SO'),
    ("SO.06", "Alert Enrichment", 2, 'SO'),
    ("SO.07", "Automated Remediation", 2, 'SO'),
    ("SO.08", "Workflow Builder", 2, 'SO'),
    ("SO.09", "Collaboration and War Room", 2, 'SO'),
    ("SO.10", "SLA Tracking", 2, 'SO'),
    ("SO.11", "Metrics and KPI Dashboard", 2, 'SO'),
    ("SO.12", "Playbook Library", 2, 'SO'),
    ("SO.13", "API Integration Hub", 2, 'SO'),
    ("SO.14", "SOAR Analytics", 2, 'SO'),
]

async def ingest_domain_soar(conn) -> int:
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
