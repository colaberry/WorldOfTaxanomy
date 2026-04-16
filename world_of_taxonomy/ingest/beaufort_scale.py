"""Ingest Beaufort Wind Force Scale."""
from __future__ import annotations

_SYSTEM_ROW = ("beaufort_scale", "Beaufort Scale", "Beaufort Wind Force Scale", "1946", "Global", "WMO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BF", "Beaufort Scale", 1, None),
    ("BF.00", "Force 0: Calm", 2, 'BF'),
    ("BF.01", "Force 1: Light Air", 2, 'BF'),
    ("BF.02", "Force 2: Light Breeze", 2, 'BF'),
    ("BF.03", "Force 3: Gentle Breeze", 2, 'BF'),
    ("BF.04", "Force 4: Moderate Breeze", 2, 'BF'),
    ("BF.05", "Force 5: Fresh Breeze", 2, 'BF'),
    ("BF.06", "Force 6: Strong Breeze", 2, 'BF'),
    ("BF.07", "Force 7: Near Gale", 2, 'BF'),
    ("BF.08", "Force 8: Gale", 2, 'BF'),
    ("BF.09", "Force 9: Strong Gale", 2, 'BF'),
    ("BF.10", "Force 10: Storm", 2, 'BF'),
    ("BF.11", "Force 11: Violent Storm", 2, 'BF'),
    ("BF.12", "Force 12: Hurricane Force", 2, 'BF'),
]

async def ingest_beaufort_scale(conn) -> int:
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
