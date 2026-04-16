"""Ingest Cold Chain Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cold_chain", "Cold Chain", "Cold Chain Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CC", "Cold Chain Types", 1, None),
    ("CC.01", "Refrigerated Truck", 2, 'CC'),
    ("CC.02", "Reefer Container", 2, 'CC'),
    ("CC.03", "Cold Storage Warehouse", 2, 'CC'),
    ("CC.04", "Blast Freezer", 2, 'CC'),
    ("CC.05", "Controlled Atmosphere Storage", 2, 'CC'),
    ("CC.06", "Cryogenic Transport", 2, 'CC'),
    ("CC.07", "Phase Change Material (PCM)", 2, 'CC'),
    ("CC.08", "Temperature Logger/IoT", 2, 'CC'),
    ("CC.09", "GDP Compliance (Pharma)", 2, 'CC'),
    ("CC.10", "HACCP Cold Chain", 2, 'CC'),
    ("CC.11", "Last-Mile Cold Delivery", 2, 'CC'),
    ("CC.12", "Vaccine Cold Chain", 2, 'CC'),
    ("CC.13", "Dry Ice Shipping", 2, 'CC'),
]

async def ingest_domain_cold_chain(conn) -> int:
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
