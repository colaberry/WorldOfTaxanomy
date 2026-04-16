"""Ingest Collective Bargaining Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_coll_bargain", "Collective Bargaining", "Collective Bargaining Types", "1.0", "Global", "ILO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CB", "Collective Bargaining Types", 1, None),
    ("CB.01", "Single-Employer Bargaining", 2, 'CB'),
    ("CB.02", "Multi-Employer Bargaining", 2, 'CB'),
    ("CB.03", "Industry-Level Bargaining", 2, 'CB'),
    ("CB.04", "National-Level Bargaining", 2, 'CB'),
    ("CB.05", "Pattern Bargaining", 2, 'CB'),
    ("CB.06", "Concessionary Bargaining", 2, 'CB'),
    ("CB.07", "Interest-Based Bargaining", 2, 'CB'),
    ("CB.08", "Good Faith Bargaining", 2, 'CB'),
    ("CB.09", "Impasse Resolution", 2, 'CB'),
    ("CB.10", "Strike/Lockout", 2, 'CB'),
    ("CB.11", "Binding Interest Arbitration", 2, 'CB'),
    ("CB.12", "CBA Administration", 2, 'CB'),
]

async def ingest_domain_coll_bargain(conn) -> int:
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
