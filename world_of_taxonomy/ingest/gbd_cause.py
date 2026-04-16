"""Ingest GBD Cause List."""
from __future__ import annotations

_SYSTEM_ROW = ("gbd_cause", "GBD Causes", "GBD Cause List", "2021", "Global", "IHME")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GBD", "GBD Level 1 Causes", 1, None),
    ("GBD.1", "Communicable, maternal, neonatal", 2, 'GBD'),
    ("GBD.2", "Non-communicable diseases", 2, 'GBD'),
    ("GBD.3", "Injuries", 2, 'GBD'),
    ("GBD.1A", "HIV/AIDS and STIs", 2, 'GBD'),
    ("GBD.1B", "Respiratory infections and TB", 2, 'GBD'),
    ("GBD.1C", "Enteric infections", 2, 'GBD'),
    ("GBD.1D", "Neglected tropical diseases", 2, 'GBD'),
    ("GBD.1E", "Malaria", 2, 'GBD'),
    ("GBD.1F", "Maternal and neonatal disorders", 2, 'GBD'),
    ("GBD.2A", "Neoplasms", 2, 'GBD'),
    ("GBD.2B", "Cardiovascular diseases", 2, 'GBD'),
    ("GBD.2C", "Chronic respiratory diseases", 2, 'GBD'),
    ("GBD.2D", "Digestive diseases", 2, 'GBD'),
    ("GBD.2E", "Neurological disorders", 2, 'GBD'),
    ("GBD.2F", "Mental disorders", 2, 'GBD'),
    ("GBD.2G", "Substance use disorders", 2, 'GBD'),
    ("GBD.2H", "Diabetes and kidney diseases", 2, 'GBD'),
    ("GBD.2I", "Skin and subcutaneous diseases", 2, 'GBD'),
    ("GBD.2J", "Musculoskeletal disorders", 2, 'GBD'),
    ("GBD.3A", "Transport injuries", 2, 'GBD'),
    ("GBD.3B", "Unintentional injuries", 2, 'GBD'),
    ("GBD.3C", "Self-harm and interpersonal violence", 2, 'GBD'),
]

async def ingest_gbd_cause(conn) -> int:
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
