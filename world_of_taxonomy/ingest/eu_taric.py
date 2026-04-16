"""Ingest EU Integrated Tariff (TARIC)."""
from __future__ import annotations

_SYSTEM_ROW = ("eu_taric", "EU TARIC", "EU Integrated Tariff (TARIC)", "2024", "European Union", "European Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("TR", "TARIC Sections", 1, None),
    ("TR.I", "Live Animals and Animal Products", 2, 'TR'),
    ("TR.II", "Vegetable Products", 2, 'TR'),
    ("TR.III", "Animal/Vegetable Fats and Oils", 2, 'TR'),
    ("TR.IV", "Prepared Foodstuffs, Beverages, Tobacco", 2, 'TR'),
    ("TR.V", "Mineral Products", 2, 'TR'),
    ("TR.VI", "Chemical Products", 2, 'TR'),
    ("TR.VII", "Plastics and Rubber", 2, 'TR'),
    ("TR.VIII", "Raw Hides, Skins, Leather", 2, 'TR'),
    ("TR.IX", "Wood and Cork Articles", 2, 'TR'),
    ("TR.X", "Pulp, Paper, Paperboard", 2, 'TR'),
    ("TR.XI", "Textiles and Textile Articles", 2, 'TR'),
    ("TR.XII", "Footwear, Headgear, Umbrellas", 2, 'TR'),
    ("TR.XIII", "Stone, Plaster, Cement, Ceramics, Glass", 2, 'TR'),
    ("TR.XIV", "Precious Metals and Stones", 2, 'TR'),
    ("TR.XV", "Base Metals", 2, 'TR'),
    ("TR.XVI", "Machinery and Mechanical Appliances", 2, 'TR'),
    ("TR.XVII", "Vehicles, Aircraft, Vessels", 2, 'TR'),
    ("TR.XVIII", "Optical, Photographic, Medical", 2, 'TR'),
    ("TR.XIX", "Arms and Ammunition", 2, 'TR'),
    ("TR.XX", "Miscellaneous Manufactured Articles", 2, 'TR'),
    ("TR.XXI", "Works of Art, Antiques", 2, 'TR'),
]

async def ingest_eu_taric(conn) -> int:
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
