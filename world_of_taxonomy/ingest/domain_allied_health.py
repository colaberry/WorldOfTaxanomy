"""Ingest Allied Health Profession Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_allied_health", "Allied Health", "Allied Health Profession Types", "1.0", "United States", "ASAHP")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AH", "Allied Health Profession Types", 1, None),
    ("AH.01", "Physical Therapy", 2, 'AH'),
    ("AH.02", "Occupational Therapy", 2, 'AH'),
    ("AH.03", "Speech-Language Pathology", 2, 'AH'),
    ("AH.04", "Respiratory Therapy", 2, 'AH'),
    ("AH.05", "Medical Laboratory Science", 2, 'AH'),
    ("AH.06", "Radiologic Technology", 2, 'AH'),
    ("AH.07", "Dietetics and Nutrition", 2, 'AH'),
    ("AH.08", "Audiology", 2, 'AH'),
    ("AH.09", "Optometry", 2, 'AH'),
    ("AH.10", "Physician Assistant", 2, 'AH'),
    ("AH.11", "Athletic Training", 2, 'AH'),
    ("AH.12", "Genetic Counseling", 2, 'AH'),
    ("AH.13", "Orthotics and Prosthetics", 2, 'AH'),
    ("AH.14", "Health Information Management", 2, 'AH'),
    ("AH.15", "Perfusion Technology", 2, 'AH'),
]

async def ingest_domain_allied_health(conn) -> int:
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
