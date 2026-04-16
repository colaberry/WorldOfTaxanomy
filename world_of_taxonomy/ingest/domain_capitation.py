"""Ingest Capitation Model Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_capitation", "Capitation Model", "Capitation Model Types", "1.0", "United States", "CMS")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CM", "Capitation Model Types", 1, None),
    ("CM.01", "Full Capitation", 2, 'CM'),
    ("CM.02", "Partial Capitation", 2, 'CM'),
    ("CM.03", "Primary Care Capitation", 2, 'CM'),
    ("CM.04", "Specialty Capitation", 2, 'CM'),
    ("CM.05", "Global Capitation", 2, 'CM'),
    ("CM.06", "Sub-Capitation", 2, 'CM'),
    ("CM.07", "Risk-Adjusted Capitation", 2, 'CM'),
    ("CM.08", "Age-Sex Capitation", 2, 'CM'),
    ("CM.09", "HCC Risk Adjustment", 2, 'CM'),
    ("CM.10", "Pediatric Capitation", 2, 'CM'),
    ("CM.11", "Behavioral Health Capitation", 2, 'CM'),
    ("CM.12", "Pharmacy Capitation", 2, 'CM'),
    ("CM.13", "Capitation Withhold", 2, 'CM'),
]

async def ingest_domain_capitation(conn) -> int:
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
