"""Ingest Global Solar UV Index Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("uv_index", "UV Index", "Global Solar UV Index Categories", "2002", "Global", "WHO/WMO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("UV", "UV Index Levels", 1, None),
    ("UV.01", "Low (0-2)", 2, 'UV'),
    ("UV.02", "Moderate (3-5)", 2, 'UV'),
    ("UV.03", "High (6-7)", 2, 'UV'),
    ("UV.04", "Very High (8-10)", 2, 'UV'),
    ("UV.05", "Extreme (11+)", 2, 'UV'),
    ("UV.06", "UVA (315-400 nm)", 2, 'UV'),
    ("UV.07", "UVB (280-315 nm)", 2, 'UV'),
    ("UV.08", "UVC (100-280 nm)", 2, 'UV'),
    ("UV.09", "Erythemal Action Spectrum", 2, 'UV'),
    ("UV.10", "Sun Protection Factor (SPF)", 2, 'UV'),
]

async def ingest_uv_index(conn) -> int:
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
