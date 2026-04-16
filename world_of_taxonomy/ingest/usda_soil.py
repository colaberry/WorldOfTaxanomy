"""Ingest USDA Soil Taxonomy Orders."""
from __future__ import annotations

_SYSTEM_ROW = ("usda_soil", "USDA Soil Taxonomy", "USDA Soil Taxonomy Orders", "2024", "United States", "USDA NRCS")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SO", "Soil Orders", 1, None),
    ("SO.01", "Alfisols", 2, 'SO'),
    ("SO.02", "Andisols", 2, 'SO'),
    ("SO.03", "Aridisols", 2, 'SO'),
    ("SO.04", "Entisols", 2, 'SO'),
    ("SO.05", "Gelisols", 2, 'SO'),
    ("SO.06", "Histosols", 2, 'SO'),
    ("SO.07", "Inceptisols", 2, 'SO'),
    ("SO.08", "Mollisols", 2, 'SO'),
    ("SO.09", "Oxisols", 2, 'SO'),
    ("SO.10", "Spodosols", 2, 'SO'),
    ("SO.11", "Ultisols", 2, 'SO'),
    ("SO.12", "Vertisols", 2, 'SO'),
]

async def ingest_usda_soil(conn) -> int:
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
