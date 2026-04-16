"""Ingest Formulary Tier Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_formulary", "Formulary Tier", "Formulary Tier Types", "1.0", "United States", "CMS")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FO", "Formulary Tier Types", 1, None),
    ("FO.01", "Tier 1 - Preferred Generic", 2, 'FO'),
    ("FO.02", "Tier 2 - Generic", 2, 'FO'),
    ("FO.03", "Tier 3 - Preferred Brand", 2, 'FO'),
    ("FO.04", "Tier 4 - Non-Preferred Brand", 2, 'FO'),
    ("FO.05", "Tier 5 - Specialty", 2, 'FO'),
    ("FO.06", "Tier 6 - Specialty (High Cost)", 2, 'FO'),
    ("FO.07", "Prior Authorization Required", 2, 'FO'),
    ("FO.08", "Step Therapy Required", 2, 'FO'),
    ("FO.09", "Quantity Limit", 2, 'FO'),
    ("FO.10", "Over-the-Counter (OTC)", 2, 'FO'),
    ("FO.11", "Preventive Drug List", 2, 'FO'),
    ("FO.12", "Specialty Mail Order", 2, 'FO'),
    ("FO.13", "Limited Distribution", 2, 'FO'),
    ("FO.14", "Formulary Exception", 2, 'FO'),
]

async def ingest_domain_formulary(conn) -> int:
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
