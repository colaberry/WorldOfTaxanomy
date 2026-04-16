"""Ingest ESCO Qualifications Framework."""
from __future__ import annotations

_SYSTEM_ROW = ("esco_qualifications", "ESCO Qualifications", "ESCO Qualifications Framework", "1.1", "European Union", "EU Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EQ", "ESCO Qualifications", 1, None),
    ("EQ.01", "Bachelor Degrees", 2, 'EQ'),
    ("EQ.02", "Master Degrees", 2, 'EQ'),
    ("EQ.03", "Doctoral Degrees", 2, 'EQ'),
    ("EQ.04", "Vocational Certificates", 2, 'EQ'),
    ("EQ.05", "Professional Diplomas", 2, 'EQ'),
    ("EQ.06", "Apprenticeship Certificates", 2, 'EQ'),
    ("EQ.07", "Continuing Education Credits", 2, 'EQ'),
    ("EQ.08", "Short-Cycle Tertiary", 2, 'EQ'),
    ("EQ.09", "Post-Secondary Non-Tertiary", 2, 'EQ'),
    ("EQ.10", "Upper Secondary Certificates", 2, 'EQ'),
    ("EQ.11", "Recognition of Prior Learning", 2, 'EQ'),
    ("EQ.12", "Joint Degrees", 2, 'EQ'),
    ("EQ.13", "Micro-Credentials (EU)", 2, 'EQ'),
    ("EQ.14", "Qualification Frameworks (NQF)", 2, 'EQ'),
]

async def ingest_esco_qualifications(conn) -> int:
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
