"""Ingest GCC Common Customs Tariff."""
from __future__ import annotations

_SYSTEM_ROW = ("gcc_tariff", "GCC Tariff", "GCC Common Customs Tariff", "2022", "Gulf States", "GCC Secretariat")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GC", "GCC Tariff Sections", 1, None),
    ("GC.01", "Live Animals", 2, 'GC'),
    ("GC.02", "Vegetable Products", 2, 'GC'),
    ("GC.03", "Fats and Oils", 2, 'GC'),
    ("GC.04", "Prepared Foodstuffs", 2, 'GC'),
    ("GC.05", "Mineral Products", 2, 'GC'),
    ("GC.06", "Chemical Products", 2, 'GC'),
    ("GC.07", "Plastics", 2, 'GC'),
    ("GC.08", "Hides and Leather", 2, 'GC'),
    ("GC.09", "Wood", 2, 'GC'),
    ("GC.10", "Paper", 2, 'GC'),
    ("GC.11", "Textiles", 2, 'GC'),
    ("GC.12", "Footwear", 2, 'GC'),
    ("GC.13", "Ceramics and Glass", 2, 'GC'),
    ("GC.14", "Base Metals", 2, 'GC'),
    ("GC.15", "Machinery", 2, 'GC'),
    ("GC.16", "Vehicles", 2, 'GC'),
]

async def ingest_gcc_tariff(conn) -> int:
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
