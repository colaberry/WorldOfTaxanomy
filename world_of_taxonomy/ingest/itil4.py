"""Ingest ITIL 4 Practice Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("itil4", "ITIL 4", "ITIL 4 Practice Categories", "4", "Global", "PeopleCert/Axelos")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IL", "ITIL 4 Practices", 1, None),
    ("IL.01", "General: Architecture Management", 2, 'IL'),
    ("IL.02", "General: Continual Improvement", 2, 'IL'),
    ("IL.03", "General: Information Security Management", 2, 'IL'),
    ("IL.04", "General: Knowledge Management", 2, 'IL'),
    ("IL.05", "General: Measurement and Reporting", 2, 'IL'),
    ("IL.06", "General: Organizational Change Management", 2, 'IL'),
    ("IL.07", "General: Portfolio Management", 2, 'IL'),
    ("IL.08", "General: Project Management", 2, 'IL'),
    ("IL.09", "General: Relationship Management", 2, 'IL'),
    ("IL.10", "General: Risk Management", 2, 'IL'),
    ("IL.11", "General: Service Financial Management", 2, 'IL'),
    ("IL.12", "General: Strategy Management", 2, 'IL'),
    ("IL.13", "General: Supplier Management", 2, 'IL'),
    ("IL.14", "General: Workforce and Talent Management", 2, 'IL'),
    ("IL.15", "Service Mgmt: Availability Management", 2, 'IL'),
    ("IL.16", "Service Mgmt: Business Analysis", 2, 'IL'),
    ("IL.17", "Service Mgmt: Capacity/Performance Management", 2, 'IL'),
    ("IL.18", "Service Mgmt: Change Enablement", 2, 'IL'),
    ("IL.19", "Service Mgmt: Incident Management", 2, 'IL'),
    ("IL.20", "Service Mgmt: Problem Management", 2, 'IL'),
    ("IL.21", "Service Mgmt: Service Desk", 2, 'IL'),
    ("IL.22", "Service Mgmt: Service Level Management", 2, 'IL'),
    ("IL.23", "Technical: Deployment Management", 2, 'IL'),
    ("IL.24", "Technical: Infrastructure/Platform Management", 2, 'IL'),
    ("IL.25", "Technical: Software Development and Management", 2, 'IL'),
]

async def ingest_itil4(conn) -> int:
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
