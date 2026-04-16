"""Ingest EEO Category Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_eeo_category", "EEO Category", "EEO Category Types", "1.0", "United States", "EEOC")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EE", "EEO Category Types", 1, None),
    ("EE.01", "Race and Ethnicity", 2, 'EE'),
    ("EE.02", "Gender", 2, 'EE'),
    ("EE.03", "Age (ADEA Protected)", 2, 'EE'),
    ("EE.04", "Disability Status (ADA)", 2, 'EE'),
    ("EE.05", "Veteran Status", 2, 'EE'),
    ("EE.06", "Religion", 2, 'EE'),
    ("EE.07", "National Origin", 2, 'EE'),
    ("EE.08", "Pregnancy Status", 2, 'EE'),
    ("EE.09", "Genetic Information (GINA)", 2, 'EE'),
    ("EE.10", "Sexual Orientation", 2, 'EE'),
    ("EE.11", "Gender Identity", 2, 'EE'),
    ("EE.12", "EEO-1 Report Category", 2, 'EE'),
    ("EE.13", "Adverse Impact Analysis", 2, 'EE'),
]

async def ingest_domain_eeo_category(conn) -> int:
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
