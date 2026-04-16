"""Ingest Emission Factor Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_emission_factor", "Emission Factor", "Emission Factor Types", "1.0", "Global", "IPCC/EPA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EF", "Emission Factor Types", 1, None),
    ("EF.01", "Scope 1 Direct Emission", 2, 'EF'),
    ("EF.02", "Scope 2 Location-Based", 2, 'EF'),
    ("EF.03", "Scope 2 Market-Based", 2, 'EF'),
    ("EF.04", "Scope 3 Category Factor", 2, 'EF'),
    ("EF.05", "Grid Emission Factor", 2, 'EF'),
    ("EF.06", "Fuel Combustion Factor", 2, 'EF'),
    ("EF.07", "Process Emission Factor", 2, 'EF'),
    ("EF.08", "Fugitive Emission Factor", 2, 'EF'),
    ("EF.09", "Life Cycle Emission Factor", 2, 'EF'),
    ("EF.10", "GWP-Weighted Factor", 2, 'EF'),
    ("EF.11", "Methane Emission Factor", 2, 'EF'),
    ("EF.12", "N2O Emission Factor", 2, 'EF'),
    ("EF.13", "Country-Specific Factor", 2, 'EF'),
]

async def ingest_domain_emission_factor(conn) -> int:
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
