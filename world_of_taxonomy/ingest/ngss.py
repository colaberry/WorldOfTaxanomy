"""Ingest Next Generation Science Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("ngss", "NGSS", "Next Generation Science Standards", "2013", "United States", "Achieve Inc.")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NG", "NGSS Domains", 1, None),
    ("NG.01", "Physical Sciences (PS)", 2, 'NG'),
    ("NG.02", "Life Sciences (LS)", 2, 'NG'),
    ("NG.03", "Earth and Space Sciences (ESS)", 2, 'NG'),
    ("NG.04", "Engineering, Technology, Applications (ETS)", 2, 'NG'),
    ("NG.05", "Crosscutting Concepts", 2, 'NG'),
    ("NG.06", "Science and Engineering Practices", 2, 'NG'),
    ("NG.07", "Disciplinary Core Ideas (K-2)", 2, 'NG'),
    ("NG.08", "Disciplinary Core Ideas (3-5)", 2, 'NG'),
    ("NG.09", "Disciplinary Core Ideas (6-8)", 2, 'NG'),
    ("NG.10", "Disciplinary Core Ideas (9-12)", 2, 'NG'),
    ("NG.11", "Performance Expectations", 2, 'NG'),
    ("NG.12", "Assessment Boundaries", 2, 'NG'),
    ("NG.13", "Connections to CCSS", 2, 'NG'),
]

async def ingest_ngss(conn) -> int:
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
