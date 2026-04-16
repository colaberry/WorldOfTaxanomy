"""Ingest Bristol Stool Form Scale."""
from __future__ import annotations

_SYSTEM_ROW = ("bristol_stool", "Bristol Stool Scale", "Bristol Stool Form Scale", "1997", "Global", "Lewis/Heaton")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BS", "Bristol Stool Types", 1, None),
    ("BS.01", "Type 1: Separate Hard Lumps", 2, 'BS'),
    ("BS.02", "Type 2: Lumpy Sausage", 2, 'BS'),
    ("BS.03", "Type 3: Sausage with Cracks", 2, 'BS'),
    ("BS.04", "Type 4: Smooth Soft Sausage (Ideal)", 2, 'BS'),
    ("BS.05", "Type 5: Soft Blobs", 2, 'BS'),
    ("BS.06", "Type 6: Fluffy/Mushy Pieces", 2, 'BS'),
    ("BS.07", "Type 7: Entirely Liquid", 2, 'BS'),
    ("BS.08", "Constipation Indicator (1-2)", 2, 'BS'),
    ("BS.09", "Normal Range (3-5)", 2, 'BS'),
    ("BS.10", "Diarrhea Indicator (6-7)", 2, 'BS'),
]

async def ingest_bristol_stool(conn) -> int:
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
