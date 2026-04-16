"""Ingest Cogeneration Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cogeneration", "Cogeneration", "Cogeneration Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CG", "Cogeneration Types", 1, None),
    ("CG.01", "Steam Turbine CHP", 2, 'CG'),
    ("CG.02", "Gas Turbine CHP", 2, 'CG'),
    ("CG.03", "Reciprocating Engine CHP", 2, 'CG'),
    ("CG.04", "Fuel Cell CHP", 2, 'CG'),
    ("CG.05", "Micro-CHP (Residential)", 2, 'CG'),
    ("CG.06", "Industrial CHP", 2, 'CG'),
    ("CG.07", "Combined Cycle CHP", 2, 'CG'),
    ("CG.08", "Trigeneration (CCHP)", 2, 'CG'),
    ("CG.09", "Biomass CHP", 2, 'CG'),
    ("CG.10", "Waste-to-Energy CHP", 2, 'CG'),
    ("CG.11", "CHP Efficiency Rating", 2, 'CG'),
    ("CG.12", "CHP Grid Interconnection", 2, 'CG'),
]

async def ingest_domain_cogeneration(conn) -> int:
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
