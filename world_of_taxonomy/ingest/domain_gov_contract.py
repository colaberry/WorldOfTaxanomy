"""Ingest Government Contract Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_gov_contract", "Government Contract Type", "Government Contract Types", "1.0", "United States", "FAR")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GC", "Government Contract Types", 1, None),
    ("GC.01", "Firm-Fixed-Price (FFP)", 2, 'GC'),
    ("GC.02", "Cost-Plus-Fixed-Fee", 2, 'GC'),
    ("GC.03", "Cost-Plus-Incentive-Fee", 2, 'GC'),
    ("GC.04", "Time-and-Materials (T&M)", 2, 'GC'),
    ("GC.05", "Labor-Hour Contract", 2, 'GC'),
    ("GC.06", "Indefinite Delivery/Indefinite Quantity", 2, 'GC'),
    ("GC.07", "Blanket Purchase Agreement", 2, 'GC'),
    ("GC.08", "GSA Schedule Contract", 2, 'GC'),
    ("GC.09", "Small Business Set-Aside", 2, 'GC'),
    ("GC.10", "8(a) Program Contract", 2, 'GC'),
    ("GC.11", "HUBZone Contract", 2, 'GC'),
    ("GC.12", "Service-Disabled Veteran", 2, 'GC'),
    ("GC.13", "Sole Source Contract", 2, 'GC'),
    ("GC.14", "Best Value Procurement", 2, 'GC'),
]

async def ingest_domain_gov_contract(conn) -> int:
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
