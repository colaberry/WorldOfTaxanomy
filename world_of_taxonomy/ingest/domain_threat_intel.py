"""Ingest Threat Intelligence Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_threat_intel", "Threat Intelligence", "Threat Intelligence Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("TI", "Threat Intelligence Types", 1, None),
    ("TI.01", "Strategic Threat Intelligence", 2, 'TI'),
    ("TI.02", "Tactical Threat Intelligence", 2, 'TI'),
    ("TI.03", "Operational Threat Intelligence", 2, 'TI'),
    ("TI.04", "Technical Indicators (IOC)", 2, 'TI'),
    ("TI.05", "STIX/TAXII Feed", 2, 'TI'),
    ("TI.06", "MITRE ATT&CK Mapping", 2, 'TI'),
    ("TI.07", "Open Source Intelligence (OSINT)", 2, 'TI'),
    ("TI.08", "Dark Web Monitoring", 2, 'TI'),
    ("TI.09", "Threat Actor Profiling", 2, 'TI'),
    ("TI.10", "Campaign Tracking", 2, 'TI'),
    ("TI.11", "Malware Analysis", 2, 'TI'),
    ("TI.12", "Vulnerability Intelligence", 2, 'TI'),
    ("TI.13", "Industry-Specific Threat Feed", 2, 'TI'),
    ("TI.14", "Threat Scoring", 2, 'TI'),
    ("TI.15", "Intelligence Sharing (ISAC)", 2, 'TI'),
]

async def ingest_domain_threat_intel(conn) -> int:
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
