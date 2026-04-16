"""Ingest Pharmacy Practice Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_pharma_practice", "Pharmacy Practice", "Pharmacy Practice Types", "1.0", "United States", "ASHP")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PH", "Pharmacy Practice Types", 1, None),
    ("PH.01", "Community Pharmacy", 2, 'PH'),
    ("PH.02", "Hospital Pharmacy", 2, 'PH'),
    ("PH.03", "Clinical Pharmacy", 2, 'PH'),
    ("PH.04", "Ambulatory Care Pharmacy", 2, 'PH'),
    ("PH.05", "Compounding Pharmacy", 2, 'PH'),
    ("PH.06", "Nuclear Pharmacy", 2, 'PH'),
    ("PH.07", "Specialty Pharmacy", 2, 'PH'),
    ("PH.08", "Mail-Order Pharmacy", 2, 'PH'),
    ("PH.09", "Informatics Pharmacy", 2, 'PH'),
    ("PH.10", "Oncology Pharmacy", 2, 'PH'),
    ("PH.11", "Psychiatric Pharmacy", 2, 'PH'),
    ("PH.12", "Geriatric Pharmacy", 2, 'PH'),
    ("PH.13", "Pediatric Pharmacy", 2, 'PH'),
    ("PH.14", "Pharmacogenomics", 2, 'PH'),
    ("PH.15", "Managed Care Pharmacy", 2, 'PH'),
]

async def ingest_domain_pharma_practice(conn) -> int:
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
