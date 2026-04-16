"""Ingest IMO Ship Type Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("imo_ship_type", "IMO Ship Types", "IMO Ship Type Classification", "2024", "Global", "IMO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IS", "IMO Ship Types", 1, None),
    ("IS.01", "Oil Tanker", 2, 'IS'),
    ("IS.02", "Chemical Tanker", 2, 'IS'),
    ("IS.03", "LNG Carrier", 2, 'IS'),
    ("IS.04", "LPG Carrier", 2, 'IS'),
    ("IS.05", "Bulk Carrier", 2, 'IS'),
    ("IS.06", "General Cargo Ship", 2, 'IS'),
    ("IS.07", "Container Ship", 2, 'IS'),
    ("IS.08", "Ro-Ro Ship", 2, 'IS'),
    ("IS.09", "Passenger Ship", 2, 'IS'),
    ("IS.10", "Cruise Ship", 2, 'IS'),
    ("IS.11", "Fishing Vessel", 2, 'IS'),
    ("IS.12", "Offshore Supply Vessel", 2, 'IS'),
    ("IS.13", "Tug", 2, 'IS'),
    ("IS.14", "Dredger", 2, 'IS'),
    ("IS.15", "Naval Vessel", 2, 'IS'),
    ("IS.16", "Yacht/Pleasure Craft", 2, 'IS'),
]

async def ingest_imo_ship_type(conn) -> int:
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
