"""Ingest Purple Team Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_purple_team", "Purple Team", "Purple Team Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PU", "Purple Team Types", 1, None),
    ("PU.01", "Collaborative Exercise", 2, 'PU'),
    ("PU.02", "Detection Gap Analysis", 2, 'PU'),
    ("PU.03", "ATT&CK Coverage Assessment", 2, 'PU'),
    ("PU.04", "Control Validation", 2, 'PU'),
    ("PU.05", "Attack Simulation with Detection", 2, 'PU'),
    ("PU.06", "Playbook Development", 2, 'PU'),
    ("PU.07", "Continuous Improvement Cycle", 2, 'PU'),
    ("PU.08", "Metrics-Driven Testing", 2, 'PU'),
    ("PU.09", "Automated Purple Team", 2, 'PU'),
    ("PU.10", "Threat-Informed Defense", 2, 'PU'),
    ("PU.11", "Security Control Matrix", 2, 'PU'),
    ("PU.12", "Purple Team Reporting", 2, 'PU'),
    ("PU.13", "Executive Briefing", 2, 'PU'),
]

async def ingest_domain_purple_team(conn) -> int:
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
