"""Ingest Remote Monitoring Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_remote_monitor", "Remote Monitoring", "Remote Monitoring Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RM", "Remote Monitoring Types", 1, None),
    ("RM.01", "Cardiac Remote Monitoring", 2, 'RM'),
    ("RM.02", "Glucose Monitoring (CGM)", 2, 'RM'),
    ("RM.03", "Blood Pressure Monitoring", 2, 'RM'),
    ("RM.04", "Pulse Oximetry Monitoring", 2, 'RM'),
    ("RM.05", "Weight and BMI Monitoring", 2, 'RM'),
    ("RM.06", "Respiratory Monitoring", 2, 'RM'),
    ("RM.07", "Sleep Monitoring", 2, 'RM'),
    ("RM.08", "Activity and Mobility Tracking", 2, 'RM'),
    ("RM.09", "Fall Detection System", 2, 'RM'),
    ("RM.10", "Medication Adherence Monitor", 2, 'RM'),
    ("RM.11", "Wearable ECG Monitor", 2, 'RM'),
    ("RM.12", "Implantable Monitor", 2, 'RM'),
    ("RM.13", "Alert and Notification System", 2, 'RM'),
    ("RM.14", "Data Integration Platform", 2, 'RM'),
]

async def ingest_domain_remote_monitor(conn) -> int:
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
