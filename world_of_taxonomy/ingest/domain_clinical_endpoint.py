"""Ingest Clinical Endpoint Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_clinical_endpoint", "Clinical Endpoint", "Clinical Endpoint Types", "1.0", "Global", "FDA/EMA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CE", "Clinical Endpoint Types", 1, None),
    ("CE.01", "Primary Endpoint", 2, 'CE'),
    ("CE.02", "Secondary Endpoint", 2, 'CE'),
    ("CE.03", "Surrogate Endpoint", 2, 'CE'),
    ("CE.04", "Composite Endpoint", 2, 'CE'),
    ("CE.05", "Patient-Reported Outcome (PRO)", 2, 'CE'),
    ("CE.06", "Overall Survival", 2, 'CE'),
    ("CE.07", "Progression-Free Survival", 2, 'CE'),
    ("CE.08", "Objective Response Rate", 2, 'CE'),
    ("CE.09", "Time-to-Event Endpoint", 2, 'CE'),
    ("CE.10", "Biomarker Endpoint", 2, 'CE'),
    ("CE.11", "Safety Endpoint", 2, 'CE'),
    ("CE.12", "Quality of Life Endpoint", 2, 'CE'),
    ("CE.13", "Digital Endpoint", 2, 'CE'),
    ("CE.14", "Real-World Endpoint", 2, 'CE'),
]

async def ingest_domain_clinical_endpoint(conn) -> int:
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
