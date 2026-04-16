"""Ingest HCPCS Level III Local Codes (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("hcpcs_l3", "HCPCS Level III", "HCPCS Level III Local Codes (Skeleton)", "2024", "United States", "CMS")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("H3", "HCPCS L3 Categories", 1, None),
    ("H3.01", "Local Carrier Codes", 2, 'H3'),
    ("H3.02", "DME Regional Codes", 2, 'H3'),
    ("H3.03", "State-Specific Modifier Codes", 2, 'H3'),
    ("H3.04", "Home Health Codes", 2, 'H3'),
    ("H3.05", "Rehabilitation Codes", 2, 'H3'),
    ("H3.06", "Mental Health Local Codes", 2, 'H3'),
    ("H3.07", "Dental Local Codes", 2, 'H3'),
    ("H3.08", "Vision Local Codes", 2, 'H3'),
    ("H3.09", "Telehealth Local Codes", 2, 'H3'),
    ("H3.10", "Laboratory Local Codes", 2, 'H3'),
    ("H3.11", "Radiology Local Codes", 2, 'H3'),
    ("H3.12", "Anesthesia Local Codes", 2, 'H3'),
]

async def ingest_hcpcs_l3(conn) -> int:
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
