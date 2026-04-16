"""Ingest Incident Response Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_incident_resp", "Incident Response", "Incident Response Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IR", "Incident Response Types", 1, None),
    ("IR.01", "Preparation Phase", 2, 'IR'),
    ("IR.02", "Detection and Analysis", 2, 'IR'),
    ("IR.03", "Containment Strategy", 2, 'IR'),
    ("IR.04", "Eradication", 2, 'IR'),
    ("IR.05", "Recovery", 2, 'IR'),
    ("IR.06", "Post-Incident Review", 2, 'IR'),
    ("IR.07", "Incident Classification", 2, 'IR'),
    ("IR.08", "Escalation Procedure", 2, 'IR'),
    ("IR.09", "Communication Plan", 2, 'IR'),
    ("IR.10", "Evidence Preservation", 2, 'IR'),
    ("IR.11", "Digital Forensics", 2, 'IR'),
    ("IR.12", "Tabletop Exercise", 2, 'IR'),
    ("IR.13", "Incident Metrics (MTTD/MTTR)", 2, 'IR'),
    ("IR.14", "Ransomware Response", 2, 'IR'),
    ("IR.15", "Regulatory Notification", 2, 'IR'),
]

async def ingest_domain_incident_resp(conn) -> int:
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
