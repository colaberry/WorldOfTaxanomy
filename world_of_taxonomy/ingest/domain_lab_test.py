"""Ingest Lab Test Category Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_lab_test", "Lab Test Category", "Lab Test Category Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LT", "Lab Test Category Types", 1, None),
    ("LT.01", "Hematology Panel", 2, 'LT'),
    ("LT.02", "Chemistry Panel (CMP/BMP)", 2, 'LT'),
    ("LT.03", "Lipid Panel", 2, 'LT'),
    ("LT.04", "Liver Function Tests", 2, 'LT'),
    ("LT.05", "Thyroid Function Panel", 2, 'LT'),
    ("LT.06", "Coagulation Studies", 2, 'LT'),
    ("LT.07", "Urinalysis", 2, 'LT'),
    ("LT.08", "Microbiology Culture", 2, 'LT'),
    ("LT.09", "Immunology and Serology", 2, 'LT'),
    ("LT.10", "Molecular Diagnostics (PCR)", 2, 'LT'),
    ("LT.11", "Blood Gas Analysis", 2, 'LT'),
    ("LT.12", "Toxicology Screen", 2, 'LT'),
    ("LT.13", "Tumor Marker Panel", 2, 'LT'),
    ("LT.14", "Genetic Testing", 2, 'LT'),
    ("LT.15", "Point-of-Care Testing", 2, 'LT'),
    ("LT.16", "Flow Cytometry", 2, 'LT'),
]

async def ingest_domain_lab_test(conn) -> int:
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
