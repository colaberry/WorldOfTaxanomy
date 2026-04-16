"""Ingest NATO Codification System (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("nato_codification", "NATO Codification", "NATO Codification System (Skeleton)", "2024", "Global", "NATO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NC", "NATO Supply Groups", 1, None),
    ("NC.10", "Weapons", 2, 'NC'),
    ("NC.11", "Nuclear Ordnance", 2, 'NC'),
    ("NC.12", "Fire Control Equipment", 2, 'NC'),
    ("NC.13", "Ammunition and Explosives", 2, 'NC'),
    ("NC.14", "Guided Missiles", 2, 'NC'),
    ("NC.15", "Aircraft and Airframe Components", 2, 'NC'),
    ("NC.16", "Aircraft Components and Accessories", 2, 'NC'),
    ("NC.17", "Aircraft Launching/Landing Equipment", 2, 'NC'),
    ("NC.19", "Ships, Small Craft, Pontoons", 2, 'NC'),
    ("NC.20", "Ship and Marine Equipment", 2, 'NC'),
    ("NC.22", "Railway Equipment", 2, 'NC'),
    ("NC.23", "Ground Effect Vehicles", 2, 'NC'),
    ("NC.24", "Tractors", 2, 'NC'),
    ("NC.25", "Vehicular Equipment Components", 2, 'NC'),
    ("NC.26", "Tires and Tubes", 2, 'NC'),
    ("NC.28", "Engines and Turbines", 2, 'NC'),
    ("NC.29", "Engine Accessories", 2, 'NC'),
    ("NC.30", "Mechanical Power Transmission Equipment", 2, 'NC'),
]

async def ingest_nato_codification(conn) -> int:
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
