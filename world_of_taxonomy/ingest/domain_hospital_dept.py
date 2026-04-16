"""Ingest Hospital Department Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_hospital_dept", "Hospital Department", "Hospital Department Types", "1.0", "United States", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("HD", "Hospital Department Types", 1, None),
    ("HD.01", "Emergency Department", 2, 'HD'),
    ("HD.02", "Intensive Care Unit (ICU)", 2, 'HD'),
    ("HD.03", "Surgery Department", 2, 'HD'),
    ("HD.04", "Radiology Department", 2, 'HD'),
    ("HD.05", "Laboratory Department", 2, 'HD'),
    ("HD.06", "Pharmacy Department", 2, 'HD'),
    ("HD.07", "Maternity and Obstetrics", 2, 'HD'),
    ("HD.08", "Pediatrics Department", 2, 'HD'),
    ("HD.09", "Cardiology Department", 2, 'HD'),
    ("HD.10", "Oncology Department", 2, 'HD'),
    ("HD.11", "Orthopedics Department", 2, 'HD'),
    ("HD.12", "Neurology Department", 2, 'HD'),
    ("HD.13", "Rehabilitation Department", 2, 'HD'),
    ("HD.14", "Outpatient Clinic", 2, 'HD'),
    ("HD.15", "Administration and Support", 2, 'HD'),
    ("HD.16", "Pathology Department", 2, 'HD'),
    ("HD.17", "Respiratory Therapy", 2, 'HD'),
]

async def ingest_domain_hospital_dept(conn) -> int:
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
