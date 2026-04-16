"""Ingest UK National Qualifications Framework."""
from __future__ import annotations

_SYSTEM_ROW = ("nqf_uk", "NQF (UK)", "UK National Qualifications Framework", "2024", "United Kingdom", "Ofqual")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NQ", "NQF Levels", 1, None),
    ("NQ.01", "Entry Level 1", 2, 'NQ'),
    ("NQ.02", "Entry Level 2", 2, 'NQ'),
    ("NQ.03", "Entry Level 3", 2, 'NQ'),
    ("NQ.04", "Level 1 (GCSE D-G)", 2, 'NQ'),
    ("NQ.05", "Level 2 (GCSE A*-C)", 2, 'NQ'),
    ("NQ.06", "Level 3 (A-Level)", 2, 'NQ'),
    ("NQ.07", "Level 4 (HNC/CertHE)", 2, 'NQ'),
    ("NQ.08", "Level 5 (HND/FdA)", 2, 'NQ'),
    ("NQ.09", "Level 6 (Bachelor)", 2, 'NQ'),
    ("NQ.10", "Level 7 (Master)", 2, 'NQ'),
    ("NQ.11", "Level 8 (Doctorate)", 2, 'NQ'),
    ("NQ.12", "T Levels", 2, 'NQ'),
    ("NQ.13", "Functional Skills", 2, 'NQ'),
]

async def ingest_nqf_uk(conn) -> int:
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
