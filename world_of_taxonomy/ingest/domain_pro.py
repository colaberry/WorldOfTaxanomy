"""Ingest Patient-Reported Outcome Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_pro", "Patient-Reported Outcome", "Patient-Reported Outcome Types", "1.0", "Global", "FDA/ISOQOL")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PR", "Patient-Reported Outcome Types", 1, None),
    ("PR.01", "Generic Health Status (SF-36)", 2, 'PR'),
    ("PR.02", "Disease-Specific PRO", 2, 'PR'),
    ("PR.03", "Symptom Severity Scale", 2, 'PR'),
    ("PR.04", "Functional Status Measure", 2, 'PR'),
    ("PR.05", "Quality of Life (QoL)", 2, 'PR'),
    ("PR.06", "Patient Satisfaction Survey", 2, 'PR'),
    ("PR.07", "PROMIS Instruments", 2, 'PR'),
    ("PR.08", "Pain Assessment (VAS/NRS)", 2, 'PR'),
    ("PR.09", "Fatigue Assessment", 2, 'PR'),
    ("PR.10", "Depression Screening (PHQ-9)", 2, 'PR'),
    ("PR.11", "Anxiety Screening (GAD-7)", 2, 'PR'),
    ("PR.12", "ePRO (Electronic)", 2, 'PR'),
    ("PR.13", "PRO-CTCAE", 2, 'PR'),
    ("PR.14", "Caregiver-Reported Outcome", 2, 'PR'),
]

async def ingest_domain_pro(conn) -> int:
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
