"""Ingest O*NET Work Context Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("onet_work_context", "O*NET Work Context", "O*NET Work Context Categories", "28.0", "United States", "DOL/O*NET")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WC", "Work Context", 1, None),
    ("WC.01", "Interpersonal Relationships", 2, 'WC'),
    ("WC.02", "Physical Work Conditions", 2, 'WC'),
    ("WC.03", "Structural Job Characteristics", 2, 'WC'),
    ("WC.04", "Contact With Others", 2, 'WC'),
    ("WC.05", "Deal With External Customers", 2, 'WC'),
    ("WC.06", "Coordinate With Others", 2, 'WC'),
    ("WC.07", "Indoors/Outdoors", 2, 'WC'),
    ("WC.08", "Exposed to Hazardous Conditions", 2, 'WC'),
    ("WC.09", "Cramped Work Space", 2, 'WC'),
    ("WC.10", "Sounds/Noise Levels", 2, 'WC'),
    ("WC.11", "Very Hot/Cold Temperatures", 2, 'WC'),
    ("WC.12", "Extremely Bright/Dark", 2, 'WC'),
    ("WC.13", "Pace Determined by Equipment", 2, 'WC'),
    ("WC.14", "Time Pressure", 2, 'WC'),
]

async def ingest_onet_work_context(conn) -> int:
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
