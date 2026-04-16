"""Ingest Soil Contamination Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_soil_contam", "Soil Contamination", "Soil Contamination Types", "1.0", "Global", "EPA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SC", "Soil Contamination Types", 1, None),
    ("SC.01", "Heavy Metal Contamination", 2, 'SC'),
    ("SC.02", "Petroleum Hydrocarbon", 2, 'SC'),
    ("SC.03", "Pesticide Residue", 2, 'SC'),
    ("SC.04", "PCB Contamination", 2, 'SC'),
    ("SC.05", "Asbestos Contamination", 2, 'SC'),
    ("SC.06", "PFAS Contamination", 2, 'SC'),
    ("SC.07", "Radioactive Contamination", 2, 'SC'),
    ("SC.08", "Acid Mine Drainage", 2, 'SC'),
    ("SC.09", "Salinity and Sodicity", 2, 'SC'),
    ("SC.10", "Nitrate Leaching", 2, 'SC'),
    ("SC.11", "Microplastic in Soil", 2, 'SC'),
    ("SC.12", "Brownfield Classification", 2, 'SC'),
    ("SC.13", "Remediation Technology", 2, 'SC'),
]

async def ingest_domain_soil_contam(conn) -> int:
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
