"""Ingest ASEAN Harmonised Tariff Nomenclature."""
from __future__ import annotations

_SYSTEM_ROW = ("asean_tariff", "ASEAN Tariff", "ASEAN Harmonised Tariff Nomenclature", "2022", "Southeast Asia", "ASEAN Secretariat")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AT", "AHTN Sections", 1, None),
    ("AT.I", "Live Animals and Animal Products", 2, 'AT'),
    ("AT.II", "Vegetable Products", 2, 'AT'),
    ("AT.III", "Fats and Oils", 2, 'AT'),
    ("AT.IV", "Prepared Foodstuffs", 2, 'AT'),
    ("AT.V", "Mineral Products", 2, 'AT'),
    ("AT.VI", "Chemical Products", 2, 'AT'),
    ("AT.VII", "Plastics and Rubber", 2, 'AT'),
    ("AT.VIII", "Hides and Leather", 2, 'AT'),
    ("AT.IX", "Wood and Cork", 2, 'AT'),
    ("AT.X", "Paper Products", 2, 'AT'),
    ("AT.XI", "Textiles", 2, 'AT'),
    ("AT.XII", "Footwear", 2, 'AT'),
    ("AT.XIII", "Stone, Ceramics, Glass", 2, 'AT'),
    ("AT.XIV", "Precious Metals", 2, 'AT'),
    ("AT.XV", "Base Metals", 2, 'AT'),
    ("AT.XVI", "Machinery", 2, 'AT'),
    ("AT.XVII", "Vehicles", 2, 'AT'),
    ("AT.XVIII", "Instruments", 2, 'AT'),
]

async def ingest_asean_tariff(conn) -> int:
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
