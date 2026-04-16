"""Ingest Population Health Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_pop_health", "Population Health", "Population Health Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PO", "Population Health Types", 1, None),
    ("PO.01", "Disease Surveillance", 2, 'PO'),
    ("PO.02", "Health Risk Stratification", 2, 'PO'),
    ("PO.03", "Care Gap Analysis", 2, 'PO'),
    ("PO.04", "Chronic Disease Management", 2, 'PO'),
    ("PO.05", "Preventive Health Program", 2, 'PO'),
    ("PO.06", "Community Health Assessment", 2, 'PO'),
    ("PO.07", "Health Equity Analysis", 2, 'PO'),
    ("PO.08", "Social Needs Screening", 2, 'PO'),
    ("PO.09", "Panel Management", 2, 'PO'),
    ("PO.10", "Quality Measure Reporting", 2, 'PO'),
    ("PO.11", "Predictive Analytics", 2, 'PO'),
    ("PO.12", "Patient Attribution Model", 2, 'PO'),
    ("PO.13", "Population Health Dashboard", 2, 'PO'),
]

async def ingest_domain_pop_health(conn) -> int:
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
