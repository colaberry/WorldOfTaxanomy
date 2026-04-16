"""Ingest ICAO Airport Code Regions (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("icao_airport", "ICAO Airport Codes", "ICAO Airport Code Regions (Skeleton)", "2024", "Global", "ICAO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IA", "ICAO Regions", 1, None),
    ("IA.A", "Region A - Western South Pacific", 2, 'IA'),
    ("IA.B", "Region B - Iceland/Greenland/Kosovo", 2, 'IA'),
    ("IA.C", "Region C - Canada", 2, 'IA'),
    ("IA.D", "Region D - West Africa", 2, 'IA'),
    ("IA.E", "Region E - Northern Europe", 2, 'IA'),
    ("IA.F", "Region F - Southern Africa", 2, 'IA'),
    ("IA.G", "Region G - Southwest Pacific", 2, 'IA'),
    ("IA.H", "Region H - East Africa", 2, 'IA'),
    ("IA.K", "Region K - Contiguous USA", 2, 'IA'),
    ("IA.L", "Region L - Southern Europe", 2, 'IA'),
    ("IA.M", "Region M - Central America", 2, 'IA'),
    ("IA.N", "Region N - South Pacific", 2, 'IA'),
    ("IA.O", "Region O - Middle East", 2, 'IA'),
    ("IA.P", "Region P - North Pacific", 2, 'IA'),
    ("IA.R", "Region R - Western Pacific", 2, 'IA'),
    ("IA.S", "Region S - South America", 2, 'IA'),
    ("IA.U", "Region U - Russia/CIS", 2, 'IA'),
    ("IA.V", "Region V - South Asia", 2, 'IA'),
    ("IA.W", "Region W - Maritime SE Asia", 2, 'IA'),
    ("IA.Z", "Region Z - China/Mongolia", 2, 'IA'),
]

async def ingest_icao_airport(conn) -> int:
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
