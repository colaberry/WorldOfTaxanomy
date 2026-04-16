"""Ingest Pantone Color System Families."""
from __future__ import annotations

_SYSTEM_ROW = ("pantone_families", "Pantone Families", "Pantone Color System Families", "2024", "Global", "Pantone LLC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PA", "Pantone Families", 1, None),
    ("PA.01", "Pantone Matching System (PMS)", 2, 'PA'),
    ("PA.02", "Pantone Fashion, Home + Interiors", 2, 'PA'),
    ("PA.03", "Pantone SkinTone Guide", 2, 'PA'),
    ("PA.04", "Pantone Pastels and Neons", 2, 'PA'),
    ("PA.05", "Pantone Metallics", 2, 'PA'),
    ("PA.06", "Pantone Extended Gamut", 2, 'PA'),
    ("PA.07", "Pantone Color Bridge", 2, 'PA'),
    ("PA.08", "Pantone CMYK Guide", 2, 'PA'),
    ("PA.09", "Color of the Year", 2, 'PA'),
    ("PA.10", "Pantone Connect (Digital)", 2, 'PA'),
    ("PA.11", "Pantone Plastics Standard", 2, 'PA'),
]

async def ingest_pantone_families(conn) -> int:
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
