"""Ingest MERCOSUR Common External Tariff (NCM)."""
from __future__ import annotations

_SYSTEM_ROW = ("mercosur_tariff", "MERCOSUR Tariff", "MERCOSUR Common External Tariff (NCM)", "2022", "South America", "MERCOSUR")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MT", "NCM Sections", 1, None),
    ("MT.I", "Live Animals", 2, 'MT'),
    ("MT.II", "Vegetable Products", 2, 'MT'),
    ("MT.III", "Fats and Oils", 2, 'MT'),
    ("MT.IV", "Prepared Foodstuffs", 2, 'MT'),
    ("MT.V", "Mineral Products", 2, 'MT'),
    ("MT.VI", "Chemical Products", 2, 'MT'),
    ("MT.VII", "Plastics", 2, 'MT'),
    ("MT.VIII", "Leather", 2, 'MT'),
    ("MT.IX", "Wood", 2, 'MT'),
    ("MT.X", "Paper", 2, 'MT'),
    ("MT.XI", "Textiles", 2, 'MT'),
    ("MT.XII", "Footwear", 2, 'MT'),
    ("MT.XIII", "Ceramics and Glass", 2, 'MT'),
    ("MT.XIV", "Precious Metals", 2, 'MT'),
    ("MT.XV", "Base Metals", 2, 'MT'),
    ("MT.XVI", "Machinery", 2, 'MT'),
    ("MT.XVII", "Vehicles", 2, 'MT'),
]

async def ingest_mercosur_tariff(conn) -> int:
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
