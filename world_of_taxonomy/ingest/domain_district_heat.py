"""Ingest District Heating Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_district_heat", "District Heating", "District Heating Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DH", "District Heating Types", 1, None),
    ("DH.01", "Steam-Based District Heating", 2, 'DH'),
    ("DH.02", "Hot Water District Heating", 2, 'DH'),
    ("DH.03", "Low-Temperature District Heating (4GDH)", 2, 'DH'),
    ("DH.04", "Fifth-Generation District Heating", 2, 'DH'),
    ("DH.05", "Combined Heat and Power (CHP)", 2, 'DH'),
    ("DH.06", "Waste Heat Recovery", 2, 'DH'),
    ("DH.07", "Biomass District Heating", 2, 'DH'),
    ("DH.08", "Geothermal District Heating", 2, 'DH'),
    ("DH.09", "Solar Thermal District Heating", 2, 'DH'),
    ("DH.10", "Heat Pump District Heating", 2, 'DH'),
    ("DH.11", "Thermal Energy Storage", 2, 'DH'),
    ("DH.12", "District Cooling", 2, 'DH'),
    ("DH.13", "Heat Network Regulation", 2, 'DH'),
]

async def ingest_domain_district_heat(conn) -> int:
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
