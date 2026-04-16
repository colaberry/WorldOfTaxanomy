"""Ingest Nursing Interventions Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("nic_nursing_intv", "NIC", "Nursing Interventions Classification", "7", "Global", "University of Iowa")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NI", "NIC Domains", 1, None),
    ("NI.01", "Physiological: Basic", 2, 'NI'),
    ("NI.02", "Physiological: Complex", 2, 'NI'),
    ("NI.03", "Behavioral", 2, 'NI'),
    ("NI.04", "Safety", 2, 'NI'),
    ("NI.05", "Family", 2, 'NI'),
    ("NI.06", "Health System", 2, 'NI'),
    ("NI.07", "Community", 2, 'NI'),
    ("NI.08", "Activity and Exercise Mgmt", 2, 'NI'),
    ("NI.09", "Elimination Mgmt", 2, 'NI'),
    ("NI.10", "Immobility Mgmt", 2, 'NI'),
    ("NI.11", "Nutrition Support", 2, 'NI'),
    ("NI.12", "Physical Comfort Promotion", 2, 'NI'),
    ("NI.13", "Self-Care Facilitation", 2, 'NI'),
]

async def ingest_nic_nursing_intv(conn) -> int:
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
