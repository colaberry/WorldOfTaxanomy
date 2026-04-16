"""Ingest Earthquake Magnitude Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("richter_scale", "Richter Scale", "Earthquake Magnitude Classification", "2024", "Global", "USGS")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RS", "Earthquake Magnitudes", 1, None),
    ("RS.01", "Micro (< 2.0)", 2, 'RS'),
    ("RS.02", "Minor (2.0-3.9)", 2, 'RS'),
    ("RS.03", "Light (4.0-4.9)", 2, 'RS'),
    ("RS.04", "Moderate (5.0-5.9)", 2, 'RS'),
    ("RS.05", "Strong (6.0-6.9)", 2, 'RS'),
    ("RS.06", "Major (7.0-7.9)", 2, 'RS'),
    ("RS.07", "Great (8.0+)", 2, 'RS'),
    ("RS.08", "Moment Magnitude Scale (Mw)", 2, 'RS'),
    ("RS.09", "Surface Wave Magnitude (Ms)", 2, 'RS'),
    ("RS.10", "Body Wave Magnitude (mb)", 2, 'RS'),
    ("RS.11", "Local Magnitude (ML/Richter)", 2, 'RS'),
    ("RS.12", "Modified Mercalli Intensity (MMI)", 2, 'RS'),
]

async def ingest_richter_scale(conn) -> int:
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
