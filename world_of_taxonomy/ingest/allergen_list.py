"""Ingest EU 14 Major Food Allergens."""
from __future__ import annotations

_SYSTEM_ROW = ("allergen_list", "EU Allergen List", "EU 14 Major Food Allergens", "2024", "European Union", "EU Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AL", "EU Allergens", 1, None),
    ("AL.01", "Cereals Containing Gluten", 2, 'AL'),
    ("AL.02", "Crustaceans", 2, 'AL'),
    ("AL.03", "Eggs", 2, 'AL'),
    ("AL.04", "Fish", 2, 'AL'),
    ("AL.05", "Peanuts", 2, 'AL'),
    ("AL.06", "Soybeans", 2, 'AL'),
    ("AL.07", "Milk (Lactose)", 2, 'AL'),
    ("AL.08", "Tree Nuts", 2, 'AL'),
    ("AL.09", "Celery", 2, 'AL'),
    ("AL.10", "Mustard", 2, 'AL'),
    ("AL.11", "Sesame Seeds", 2, 'AL'),
    ("AL.12", "Sulphites (SO2 > 10mg/kg)", 2, 'AL'),
    ("AL.13", "Lupin", 2, 'AL'),
    ("AL.14", "Molluscs", 2, 'AL'),
]

async def ingest_allergen_list(conn) -> int:
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
