"""Ingest Pathology Subspecialty Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_pathology_sub", "Pathology Subspecialty", "Pathology Subspecialty Types", "1.0", "Global", "CAP")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PA", "Pathology Subspecialty Types", 1, None),
    ("PA.01", "Anatomic Pathology", 2, 'PA'),
    ("PA.02", "Clinical Pathology", 2, 'PA'),
    ("PA.03", "Surgical Pathology", 2, 'PA'),
    ("PA.04", "Cytopathology", 2, 'PA'),
    ("PA.05", "Dermatopathology", 2, 'PA'),
    ("PA.06", "Neuropathology", 2, 'PA'),
    ("PA.07", "Hematopathology", 2, 'PA'),
    ("PA.08", "Molecular Pathology", 2, 'PA'),
    ("PA.09", "Forensic Pathology", 2, 'PA'),
    ("PA.10", "Renal Pathology", 2, 'PA'),
    ("PA.11", "Gastrointestinal Pathology", 2, 'PA'),
    ("PA.12", "Pulmonary Pathology", 2, 'PA'),
    ("PA.13", "Pediatric Pathology", 2, 'PA'),
    ("PA.14", "Digital Pathology", 2, 'PA'),
]

async def ingest_domain_pathology_sub(conn) -> int:
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
