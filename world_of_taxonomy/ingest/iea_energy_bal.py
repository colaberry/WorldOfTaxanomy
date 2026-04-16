"""Ingest IEA Energy Balance Flow Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("iea_energy_bal", "IEA Energy Balance", "IEA Energy Balance Flow Categories", "2024", "Global", "IEA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IE", "IEA Energy Balance", 1, None),
    ("IE.01", "Production", 2, 'IE'),
    ("IE.02", "Imports", 2, 'IE'),
    ("IE.03", "Exports", 2, 'IE'),
    ("IE.04", "International Marine Bunkers", 2, 'IE'),
    ("IE.05", "International Aviation Bunkers", 2, 'IE'),
    ("IE.06", "Stock Changes", 2, 'IE'),
    ("IE.07", "Total Primary Energy Supply (TPES)", 2, 'IE'),
    ("IE.08", "Transfers", 2, 'IE'),
    ("IE.09", "Statistical Differences", 2, 'IE'),
    ("IE.10", "Electricity Plants", 2, 'IE'),
    ("IE.11", "CHP Plants", 2, 'IE'),
    ("IE.12", "Heat Plants", 2, 'IE'),
    ("IE.13", "Industry", 2, 'IE'),
    ("IE.14", "Transport", 2, 'IE'),
    ("IE.15", "Residential", 2, 'IE'),
    ("IE.16", "Commercial and Public Services", 2, 'IE'),
    ("IE.17", "Agriculture/Forestry", 2, 'IE'),
    ("IE.18", "Non-Energy Use", 2, 'IE'),
]

async def ingest_iea_energy_bal(conn) -> int:
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
