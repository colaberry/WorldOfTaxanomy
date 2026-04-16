"""Ingest Clinical Decision Support Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cds", "Clinical Decision Support", "Clinical Decision Support Types", "1.0", "United States", "ONC")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CS", "Clinical Decision Support Types", 1, None),
    ("CS.01", "Drug Allergy Alert", 2, 'CS'),
    ("CS.02", "Drug Interaction Alert", 2, 'CS'),
    ("CS.03", "Dosage Recommendation", 2, 'CS'),
    ("CS.04", "Order Set", 2, 'CS'),
    ("CS.05", "Clinical Pathway", 2, 'CS'),
    ("CS.06", "Diagnostic Support", 2, 'CS'),
    ("CS.07", "Risk Calculator", 2, 'CS'),
    ("CS.08", "Preventive Care Reminder", 2, 'CS'),
    ("CS.09", "Evidence-Based Guideline", 2, 'CS'),
    ("CS.10", "AI-Powered CDS", 2, 'CS'),
    ("CS.11", "Sepsis Early Warning", 2, 'CS'),
    ("CS.12", "Pharmacogenomic Alert", 2, 'CS'),
    ("CS.13", "Image Analysis CDS", 2, 'CS'),
    ("CS.14", "Interoperable CDS (CDS Hooks)", 2, 'CS'),
]

async def ingest_domain_cds(conn) -> int:
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
