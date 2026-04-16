"""Ingest Red Team Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_red_team", "Red Team", "Red Team Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RT", "Red Team Types", 1, None),
    ("RT.01", "Full-Scope Red Team", 2, 'RT'),
    ("RT.02", "Objective-Based Red Team", 2, 'RT'),
    ("RT.03", "Physical Red Team", 2, 'RT'),
    ("RT.04", "Social Engineering Red Team", 2, 'RT'),
    ("RT.05", "Adversary Simulation", 2, 'RT'),
    ("RT.06", "Assumed Breach Scenario", 2, 'RT'),
    ("RT.07", "MITRE ATT&CK Emulation", 2, 'RT'),
    ("RT.08", "Insider Threat Simulation", 2, 'RT'),
    ("RT.09", "Supply Chain Attack Simulation", 2, 'RT'),
    ("RT.10", "Cloud Red Team", 2, 'RT'),
    ("RT.11", "OT/ICS Red Team", 2, 'RT'),
    ("RT.12", "Red Team Debrief", 2, 'RT'),
    ("RT.13", "Continuous Red Team", 2, 'RT'),
    ("RT.14", "Red Team Tooling", 2, 'RT'),
]

async def ingest_domain_red_team(conn) -> int:
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
