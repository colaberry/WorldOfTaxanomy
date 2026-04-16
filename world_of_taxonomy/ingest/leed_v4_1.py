"""Ingest LEED v4.1 Green Building Rating Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("leed_v4_1", "LEED v4.1", "LEED v4.1 Green Building Rating Categories", "4.1", "Global", "USGBC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LD", "LEED Categories", 1, None),
    ("LD.01", "Integrative Process", 2, 'LD'),
    ("LD.02", "Location and Transportation", 2, 'LD'),
    ("LD.03", "Sustainable Sites", 2, 'LD'),
    ("LD.04", "Water Efficiency", 2, 'LD'),
    ("LD.05", "Energy and Atmosphere", 2, 'LD'),
    ("LD.06", "Materials and Resources", 2, 'LD'),
    ("LD.07", "Indoor Environmental Quality", 2, 'LD'),
    ("LD.08", "Innovation", 2, 'LD'),
    ("LD.09", "Regional Priority Credits", 2, 'LD'),
    ("LD.10", "Certified (40-49 points)", 2, 'LD'),
    ("LD.11", "Silver (50-59 points)", 2, 'LD'),
    ("LD.12", "Gold (60-79 points)", 2, 'LD'),
    ("LD.13", "Platinum (80+ points)", 2, 'LD'),
]

async def ingest_leed_v4_1(conn) -> int:
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
