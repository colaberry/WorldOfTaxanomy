"""Ingest Value-Based Care Model Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_vbc_model", "Value-Based Model", "Value-Based Care Model Types", "1.0", "United States", "CMS")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("VB", "Value-Based Care Model Types", 1, None),
    ("VB.01", "Accountable Care Organization (ACO)", 2, 'VB'),
    ("VB.02", "Patient-Centered Medical Home", 2, 'VB'),
    ("VB.03", "Pay-for-Performance", 2, 'VB'),
    ("VB.04", "Shared Savings Program", 2, 'VB'),
    ("VB.05", "Bundled Payment", 2, 'VB'),
    ("VB.06", "Capitation", 2, 'VB'),
    ("VB.07", "Global Budget", 2, 'VB'),
    ("VB.08", "Episode-Based Payment", 2, 'VB'),
    ("VB.09", "Quality Bonus Payment", 2, 'VB'),
    ("VB.10", "Risk-Adjusted Payment", 2, 'VB'),
    ("VB.11", "Primary Care First", 2, 'VB'),
    ("VB.12", "Direct Contracting", 2, 'VB'),
    ("VB.13", "Oncology Care Model", 2, 'VB'),
    ("VB.14", "MIPS Reporting", 2, 'VB'),
]

async def ingest_domain_vbc_model(conn) -> int:
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
