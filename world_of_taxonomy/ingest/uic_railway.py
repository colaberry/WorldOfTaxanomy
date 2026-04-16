"""Ingest UIC Railway Classification Codes."""
from __future__ import annotations

_SYSTEM_ROW = ("uic_railway", "UIC Railway Codes", "UIC Railway Classification Codes", "2024", "Global", "UIC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("UI", "UIC Code Categories", 1, None),
    ("UI.01", "Infrastructure (Track/Gauge)", 2, 'UI'),
    ("UI.02", "Signaling Systems (ERTMS/ETCS)", 2, 'UI'),
    ("UI.03", "Rolling Stock - Passenger", 2, 'UI'),
    ("UI.04", "Rolling Stock - Freight", 2, 'UI'),
    ("UI.05", "Rolling Stock - Locomotive", 2, 'UI'),
    ("UI.06", "Traction Power Systems", 2, 'UI'),
    ("UI.07", "Operations and Traffic Management", 2, 'UI'),
    ("UI.08", "Safety Standards", 2, 'UI'),
    ("UI.09", "Environmental Standards", 2, 'UI'),
    ("UI.10", "Telecommunications", 2, 'UI'),
    ("UI.11", "Interoperability (TSI)", 2, 'UI'),
    ("UI.12", "Loading Gauge Standards", 2, 'UI'),
    ("UI.13", "Noise and Vibration Standards", 2, 'UI'),
    ("UI.14", "Station Design Standards", 2, 'UI'),
]

async def ingest_uic_railway(conn) -> int:
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
