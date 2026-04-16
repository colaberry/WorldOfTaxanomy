"""Ingest FAA Aircraft Category and Class."""
from __future__ import annotations

_SYSTEM_ROW = ("faa_aircraft_cat", "FAA Aircraft Cat", "FAA Aircraft Category and Class", "2024", "United States", "FAA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FA", "FAA Categories", 1, None),
    ("FA.01", "Airplane - Single Engine Land (ASEL)", 2, 'FA'),
    ("FA.02", "Airplane - Multi Engine Land (AMEL)", 2, 'FA'),
    ("FA.03", "Airplane - Single Engine Sea (ASES)", 2, 'FA'),
    ("FA.04", "Airplane - Multi Engine Sea (AMES)", 2, 'FA'),
    ("FA.05", "Rotorcraft - Helicopter", 2, 'FA'),
    ("FA.06", "Rotorcraft - Gyroplane", 2, 'FA'),
    ("FA.07", "Glider", 2, 'FA'),
    ("FA.08", "Lighter-Than-Air - Airship", 2, 'FA'),
    ("FA.09", "Lighter-Than-Air - Balloon", 2, 'FA'),
    ("FA.10", "Powered-Lift", 2, 'FA'),
    ("FA.11", "Weight-Shift Control", 2, 'FA'),
    ("FA.12", "Powered Parachute", 2, 'FA'),
    ("FA.13", "Sport Pilot Category", 2, 'FA'),
    ("FA.14", "Experimental Category", 2, 'FA'),
    ("FA.15", "Unmanned Aircraft (UAS/Drone)", 2, 'FA'),
]

async def ingest_faa_aircraft_cat(conn) -> int:
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
