"""Ingest Nursing Specialty Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_nursing_spec", "Nursing Specialty", "Nursing Specialty Types", "1.0", "United States", "ANA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NS", "Nursing Specialty Types", 1, None),
    ("NS.01", "Critical Care Nursing", 2, 'NS'),
    ("NS.02", "Emergency Nursing", 2, 'NS'),
    ("NS.03", "Pediatric Nursing", 2, 'NS'),
    ("NS.04", "Oncology Nursing", 2, 'NS'),
    ("NS.05", "Psychiatric-Mental Health", 2, 'NS'),
    ("NS.06", "Perioperative Nursing", 2, 'NS'),
    ("NS.07", "Neonatal Nursing", 2, 'NS'),
    ("NS.08", "Gerontological Nursing", 2, 'NS'),
    ("NS.09", "Community Health Nursing", 2, 'NS'),
    ("NS.10", "Home Health Nursing", 2, 'NS'),
    ("NS.11", "Hospice and Palliative", 2, 'NS'),
    ("NS.12", "Cardiac-Vascular Nursing", 2, 'NS'),
    ("NS.13", "Nurse Practitioner", 2, 'NS'),
    ("NS.14", "Nurse Anesthetist (CRNA)", 2, 'NS'),
    ("NS.15", "Nurse Midwife", 2, 'NS'),
    ("NS.16", "Infection Control Nursing", 2, 'NS'),
]

async def ingest_domain_nursing_spec(conn) -> int:
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
