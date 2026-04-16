"""Ingest Customs Procedure Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_customs_proc", "Customs Procedure", "Customs Procedure Types", "1.0", "Global", "WCO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CU", "Customs Procedure Types", 1, None),
    ("CU.01", "Import for Home Use", 2, 'CU'),
    ("CU.02", "Export (Outright)", 2, 'CU'),
    ("CU.03", "Transit (T1/T2)", 2, 'CU'),
    ("CU.04", "Customs Warehousing", 2, 'CU'),
    ("CU.05", "Inward Processing", 2, 'CU'),
    ("CU.06", "Outward Processing", 2, 'CU'),
    ("CU.07", "Temporary Admission", 2, 'CU'),
    ("CU.08", "Re-Export", 2, 'CU'),
    ("CU.09", "Re-Import", 2, 'CU'),
    ("CU.10", "Free Zone", 2, 'CU'),
    ("CU.11", "AEO (Authorized Economic Operator)", 2, 'CU'),
    ("CU.12", "Single Window Filing", 2, 'CU'),
    ("CU.13", "Rules of Origin Determination", 2, 'CU'),
]

async def ingest_domain_customs_proc(conn) -> int:
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
