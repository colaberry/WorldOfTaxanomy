"""Ingest FAO AQUASTAT Water Use Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("fao_aquastat", "FAO AQUASTAT", "FAO AQUASTAT Water Use Categories", "2024", "Global", "FAO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY-NC-SA 3.0 IGO"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AQ", "AQUASTAT Categories", 1, None),
    ("AQ.01", "Agricultural Water Withdrawal", 2, 'AQ'),
    ("AQ.02", "Industrial Water Withdrawal", 2, 'AQ'),
    ("AQ.03", "Municipal Water Withdrawal", 2, 'AQ'),
    ("AQ.04", "Total Renewable Water Resources", 2, 'AQ'),
    ("AQ.05", "Internal Renewable Water Resources", 2, 'AQ'),
    ("AQ.06", "External Renewable Water Resources", 2, 'AQ'),
    ("AQ.07", "Desalinated Water Produced", 2, 'AQ'),
    ("AQ.08", "Treated Wastewater", 2, 'AQ'),
    ("AQ.09", "Irrigation Water Use Efficiency", 2, 'AQ'),
    ("AQ.10", "Area Equipped for Irrigation", 2, 'AQ'),
    ("AQ.11", "Dam Capacity", 2, 'AQ'),
    ("AQ.12", "Water Stress Level", 2, 'AQ'),
    ("AQ.13", "Groundwater Abstraction", 2, 'AQ'),
]

async def ingest_fao_aquastat(conn) -> int:
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
