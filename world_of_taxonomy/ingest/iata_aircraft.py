"""Ingest IATA Aircraft Type Codes."""
from __future__ import annotations

_SYSTEM_ROW = ("iata_aircraft", "IATA Aircraft", "IATA Aircraft Type Codes", "2024", "Global", "IATA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "IATA License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IATA", "IATA Aircraft Categories", 1, None),
    ("IATA.NB", "Narrowbody (Single-Aisle)", 2, 'IATA'),
    ("IATA.WB", "Widebody (Twin-Aisle)", 2, 'IATA'),
    ("IATA.RJ", "Regional Jet", 2, 'IATA'),
    ("IATA.TP", "Turboprop", 2, 'IATA'),
    ("IATA.BJ", "Business Jet", 2, 'IATA'),
    ("IATA.CG", "Cargo Aircraft", 2, 'IATA'),
    ("IATA.HE", "Helicopter", 2, 'IATA'),
    ("IATA.GA", "General Aviation", 2, 'IATA'),
    ("IATA.UL", "Ultra-Large (A380, 747)", 2, 'IATA'),
    ("IATA.SS", "Supersonic", 2, 'IATA'),
    ("IATA.EV", "Electric/eVTOL", 2, 'IATA'),
    ("IATA.AM", "Amphibious Aircraft", 2, 'IATA'),
    ("IATA.UA", "Unmanned Aerial Vehicle", 2, 'IATA'),
]

async def ingest_iata_aircraft(conn) -> int:
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
