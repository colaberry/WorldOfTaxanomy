"""Ingest Law Enforcement Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_law_enforce", "Law Enforcement", "Law Enforcement Types", "1.0", "United States", "DOJ")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LE", "Law Enforcement Types", 1, None),
    ("LE.01", "Federal Law Enforcement (FBI)", 2, 'LE'),
    ("LE.02", "Federal Law Enforcement (DEA)", 2, 'LE'),
    ("LE.03", "Federal Law Enforcement (ATF)", 2, 'LE'),
    ("LE.04", "US Marshals Service", 2, 'LE'),
    ("LE.05", "Secret Service", 2, 'LE'),
    ("LE.06", "State Police/Highway Patrol", 2, 'LE'),
    ("LE.07", "County Sheriff", 2, 'LE'),
    ("LE.08", "Municipal Police", 2, 'LE'),
    ("LE.09", "Campus Police", 2, 'LE'),
    ("LE.10", "Tribal Police", 2, 'LE'),
    ("LE.11", "Border Patrol (CBP)", 2, 'LE'),
    ("LE.12", "ICE (Immigration)", 2, 'LE'),
    ("LE.13", "Cybercrime Unit", 2, 'LE'),
]

async def ingest_domain_law_enforce(conn) -> int:
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
