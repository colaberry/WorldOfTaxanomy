"""Ingest Sterilization Method Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_sterilization", "Sterilization Method", "Sterilization Method Types", "1.0", "Global", "AAMI")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SM", "Sterilization Method Types", 1, None),
    ("SM.01", "Steam Sterilization (Autoclave)", 2, 'SM'),
    ("SM.02", "Ethylene Oxide (EtO)", 2, 'SM'),
    ("SM.03", "Hydrogen Peroxide Plasma", 2, 'SM'),
    ("SM.04", "Gamma Irradiation", 2, 'SM'),
    ("SM.05", "E-Beam Irradiation", 2, 'SM'),
    ("SM.06", "Dry Heat Sterilization", 2, 'SM'),
    ("SM.07", "Chemical Sterilization", 2, 'SM'),
    ("SM.08", "Filtration Sterilization", 2, 'SM'),
    ("SM.09", "UV Sterilization", 2, 'SM'),
    ("SM.10", "Ozone Sterilization", 2, 'SM'),
    ("SM.11", "High-Level Disinfection", 2, 'SM'),
    ("SM.12", "Biological Indicator", 2, 'SM'),
    ("SM.13", "Chemical Indicator", 2, 'SM'),
    ("SM.14", "Sterile Processing Workflow", 2, 'SM'),
]

async def ingest_domain_sterilization(conn) -> int:
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
