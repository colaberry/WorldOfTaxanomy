"""Ingest Warehouse Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_warehouse", "Warehouse Type", "Warehouse Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WH", "Warehouse Types", 1, None),
    ("WH.01", "Distribution Center", 2, 'WH'),
    ("WH.02", "Fulfillment Center", 2, 'WH'),
    ("WH.03", "Cross-Dock Facility", 2, 'WH'),
    ("WH.04", "Cold Storage Warehouse", 2, 'WH'),
    ("WH.05", "Bonded Warehouse", 2, 'WH'),
    ("WH.06", "Public Warehouse (3PL)", 2, 'WH'),
    ("WH.07", "Private Warehouse", 2, 'WH'),
    ("WH.08", "Automated/Robotic Warehouse", 2, 'WH'),
    ("WH.09", "Dark Store", 2, 'WH'),
    ("WH.10", "Micro-Fulfillment Center", 2, 'WH'),
    ("WH.11", "Hazmat Warehouse", 2, 'WH'),
    ("WH.12", "Bulk Storage Facility", 2, 'WH'),
    ("WH.13", "Sortation Center", 2, 'WH'),
]

async def ingest_domain_warehouse(conn) -> int:
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
