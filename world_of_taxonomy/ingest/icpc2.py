"""Ingest ICPC-2 (Primary Care)."""
from __future__ import annotations

_SYSTEM_ROW = ("icpc2", "ICPC-2", "ICPC-2 (Primary Care)", "2015", "Global", "WONCA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "WONCA License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ICPC", "ICPC-2 Chapters", 1, None),
    ("A", "General and Unspecified", 2, 'ICPC'),
    ("B", "Blood and Blood-Forming Organs", 2, 'ICPC'),
    ("D", "Digestive", 2, 'ICPC'),
    ("F", "Eye", 2, 'ICPC'),
    ("H", "Ear", 2, 'ICPC'),
    ("K", "Cardiovascular", 2, 'ICPC'),
    ("L", "Musculoskeletal", 2, 'ICPC'),
    ("N", "Neurological", 2, 'ICPC'),
    ("P", "Psychological", 2, 'ICPC'),
    ("R", "Respiratory", 2, 'ICPC'),
    ("S", "Skin", 2, 'ICPC'),
    ("T", "Endocrine/Metabolic", 2, 'ICPC'),
    ("U", "Urological", 2, 'ICPC'),
    ("W", "Pregnancy/Family Planning", 2, 'ICPC'),
    ("X", "Female Genital", 2, 'ICPC'),
    ("Y", "Male Genital", 2, 'ICPC'),
    ("Z", "Social Problems", 2, 'ICPC'),
]

async def ingest_icpc2(conn) -> int:
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
