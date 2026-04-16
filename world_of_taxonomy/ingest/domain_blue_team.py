"""Ingest Blue Team Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_blue_team", "Blue Team", "Blue Team Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BT", "Blue Team Types", 1, None),
    ("BT.01", "Security Operations Center (SOC)", 2, 'BT'),
    ("BT.02", "Threat Hunting", 2, 'BT'),
    ("BT.03", "Log Analysis and Monitoring", 2, 'BT'),
    ("BT.04", "Endpoint Detection (EDR)", 2, 'BT'),
    ("BT.05", "Network Detection (NDR)", 2, 'BT'),
    ("BT.06", "Email Security", 2, 'BT'),
    ("BT.07", "DNS Security", 2, 'BT'),
    ("BT.08", "Web Application Firewall", 2, 'BT'),
    ("BT.09", "Intrusion Prevention (IPS)", 2, 'BT'),
    ("BT.10", "Data Loss Prevention", 2, 'BT'),
    ("BT.11", "Security Awareness Training", 2, 'BT'),
    ("BT.12", "Vulnerability Remediation", 2, 'BT'),
    ("BT.13", "Threat Intelligence Consumption", 2, 'BT'),
    ("BT.14", "Blue Team Automation", 2, 'BT'),
    ("BT.15", "Deception Technology", 2, 'BT'),
]

async def ingest_domain_blue_team(conn) -> int:
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
