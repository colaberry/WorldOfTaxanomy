"""Ingest ICAO Flight Rule and Airspace Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("icao_doc4444", "ICAO Flight Rules", "ICAO Flight Rule and Airspace Categories", "2024", "Global", "ICAO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FR", "ICAO Flight/Airspace", 1, None),
    ("FR.01", "VFR (Visual Flight Rules)", 2, 'FR'),
    ("FR.02", "IFR (Instrument Flight Rules)", 2, 'FR'),
    ("FR.03", "SVFR (Special VFR)", 2, 'FR'),
    ("FR.04", "Class A Airspace", 2, 'FR'),
    ("FR.05", "Class B Airspace", 2, 'FR'),
    ("FR.06", "Class C Airspace", 2, 'FR'),
    ("FR.07", "Class D Airspace", 2, 'FR'),
    ("FR.08", "Class E Airspace", 2, 'FR'),
    ("FR.09", "Class F Airspace", 2, 'FR'),
    ("FR.10", "Class G Airspace", 2, 'FR'),
    ("FR.11", "Prohibited Area", 2, 'FR'),
    ("FR.12", "Restricted Area", 2, 'FR'),
    ("FR.13", "Danger Area", 2, 'FR'),
    ("FR.14", "RVSM (Reduced Vertical Separation)", 2, 'FR'),
]

async def ingest_icao_doc4444(conn) -> int:
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
