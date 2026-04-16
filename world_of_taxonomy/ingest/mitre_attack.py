"""Ingest MITRE ATT&CK Framework Tactics."""
from __future__ import annotations

_SYSTEM_ROW = ("mitre_attack", "MITRE ATT&CK", "MITRE ATT&CK Framework Tactics", "14.1", "Global", "MITRE Corporation")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Apache 2.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MA", "ATT&CK Tactics", 1, None),
    ("MA.01", "Reconnaissance", 2, 'MA'),
    ("MA.02", "Resource Development", 2, 'MA'),
    ("MA.03", "Initial Access", 2, 'MA'),
    ("MA.04", "Execution", 2, 'MA'),
    ("MA.05", "Persistence", 2, 'MA'),
    ("MA.06", "Privilege Escalation", 2, 'MA'),
    ("MA.07", "Defense Evasion", 2, 'MA'),
    ("MA.08", "Credential Access", 2, 'MA'),
    ("MA.09", "Discovery", 2, 'MA'),
    ("MA.10", "Lateral Movement", 2, 'MA'),
    ("MA.11", "Collection", 2, 'MA'),
    ("MA.12", "Command and Control", 2, 'MA'),
    ("MA.13", "Exfiltration", 2, 'MA'),
    ("MA.14", "Impact", 2, 'MA'),
]

async def ingest_mitre_attack(conn) -> int:
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
