"""Ingest Emergency Service Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_emergency_svc", "Emergency Service", "Emergency Service Types", "1.0", "United States", "FEMA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ES", "Emergency Service Types", 1, None),
    ("ES.01", "Fire Department", 2, 'ES'),
    ("ES.02", "Emergency Medical Services", 2, 'ES'),
    ("ES.03", "Search and Rescue", 2, 'ES'),
    ("ES.04", "Hazmat Response", 2, 'ES'),
    ("ES.05", "Disaster Relief (FEMA)", 2, 'ES'),
    ("ES.06", "Emergency Management Agency", 2, 'ES'),
    ("ES.07", "911 Dispatch Center", 2, 'ES'),
    ("ES.08", "Emergency Operations Center", 2, 'ES'),
    ("ES.09", "Community Emergency Response (CERT)", 2, 'ES'),
    ("ES.10", "Mutual Aid Agreement", 2, 'ES'),
    ("ES.11", "National Guard Activation", 2, 'ES'),
    ("ES.12", "Emergency Alert System", 2, 'ES'),
    ("ES.13", "Crisis Communication", 2, 'ES'),
]

async def ingest_domain_emergency_svc(conn) -> int:
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
