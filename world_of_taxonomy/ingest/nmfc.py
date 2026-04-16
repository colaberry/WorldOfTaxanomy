"""Ingest National Motor Freight Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("nmfc", "NMFC", "National Motor Freight Classification", "2024", "United States", "NMFTA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NM", "NMFC Classes", 1, None),
    ("NM.50", "Class 50 (Fits on pallet, densest)", 2, 'NM'),
    ("NM.55", "Class 55", 2, 'NM'),
    ("NM.60", "Class 60", 2, 'NM'),
    ("NM.65", "Class 65", 2, 'NM'),
    ("NM.70", "Class 70", 2, 'NM'),
    ("NM.77", "Class 77.5", 2, 'NM'),
    ("NM.85", "Class 85", 2, 'NM'),
    ("NM.92", "Class 92.5", 2, 'NM'),
    ("NM.100", "Class 100", 2, 'NM'),
    ("NM.110", "Class 110", 2, 'NM'),
    ("NM.125", "Class 125", 2, 'NM'),
    ("NM.150", "Class 150", 2, 'NM'),
    ("NM.175", "Class 175", 2, 'NM'),
    ("NM.200", "Class 200", 2, 'NM'),
    ("NM.250", "Class 250", 2, 'NM'),
    ("NM.300", "Class 300", 2, 'NM'),
    ("NM.400", "Class 400", 2, 'NM'),
    ("NM.500", "Class 500 (Lightest/bulkiest)", 2, 'NM'),
]

async def ingest_nmfc(conn) -> int:
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
