"""Ingest NFPA Fire Protection Code Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("nfpa_codes", "NFPA Codes", "NFPA Fire Protection Code Categories", "2024", "United States", "NFPA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NF", "NFPA Code Categories", 1, None),
    ("NF.01", "NFPA 1 - Fire Code", 2, 'NF'),
    ("NF.02", "NFPA 10 - Portable Fire Extinguishers", 2, 'NF'),
    ("NF.03", "NFPA 13 - Sprinkler Systems", 2, 'NF'),
    ("NF.04", "NFPA 25 - Inspection/Testing Water Systems", 2, 'NF'),
    ("NF.05", "NFPA 30 - Flammable Liquids Code", 2, 'NF'),
    ("NF.06", "NFPA 54 - National Fuel Gas Code", 2, 'NF'),
    ("NF.07", "NFPA 70 - National Electrical Code", 2, 'NF'),
    ("NF.08", "NFPA 72 - Fire Alarm Code", 2, 'NF'),
    ("NF.09", "NFPA 80 - Fire Doors and Windows", 2, 'NF'),
    ("NF.10", "NFPA 90A - HVAC Systems", 2, 'NF'),
    ("NF.11", "NFPA 101 - Life Safety Code", 2, 'NF'),
    ("NF.12", "NFPA 110 - Emergency Power", 2, 'NF'),
    ("NF.13", "NFPA 704 - Hazard Diamond", 2, 'NF'),
    ("NF.14", "NFPA 1500 - Firefighter Safety", 2, 'NF'),
    ("NF.15", "NFPA 1600 - Emergency Management", 2, 'NF'),
    ("NF.16", "NFPA 2001 - Clean Agent Fire Suppression", 2, 'NF'),
]

async def ingest_nfpa_codes(conn) -> int:
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
