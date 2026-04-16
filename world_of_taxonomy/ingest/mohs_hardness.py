"""Ingest Mohs Scale of Mineral Hardness."""
from __future__ import annotations

_SYSTEM_ROW = ("mohs_hardness", "Mohs Hardness", "Mohs Scale of Mineral Hardness", "1822", "Global", "Friedrich Mohs")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MH", "Mohs Scale", 1, None),
    ("MH.01", "1: Talc", 2, 'MH'),
    ("MH.02", "2: Gypsum", 2, 'MH'),
    ("MH.03", "3: Calcite", 2, 'MH'),
    ("MH.04", "4: Fluorite", 2, 'MH'),
    ("MH.05", "5: Apatite", 2, 'MH'),
    ("MH.06", "6: Orthoclase", 2, 'MH'),
    ("MH.07", "7: Quartz", 2, 'MH'),
    ("MH.08", "8: Topaz", 2, 'MH'),
    ("MH.09", "9: Corundum", 2, 'MH'),
    ("MH.10", "10: Diamond", 2, 'MH'),
]

async def ingest_mohs_hardness(conn) -> int:
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
