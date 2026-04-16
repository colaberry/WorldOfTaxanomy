"""Ingest O*NET Work Value Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("onet_work_values", "O*NET Work Values", "O*NET Work Value Categories", "28.0", "United States", "DOL/O*NET")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("OV", "O*NET Work Values", 1, None),
    ("OV.01", "Achievement", 2, 'OV'),
    ("OV.02", "Working Conditions", 2, 'OV'),
    ("OV.03", "Recognition", 2, 'OV'),
    ("OV.04", "Relationships", 2, 'OV'),
    ("OV.05", "Support", 2, 'OV'),
    ("OV.06", "Independence", 2, 'OV'),
    ("OV.07", "Ability Utilization", 2, 'OV'),
    ("OV.08", "Compensation", 2, 'OV'),
    ("OV.09", "Security", 2, 'OV'),
    ("OV.10", "Advancement", 2, 'OV'),
    ("OV.11", "Creativity", 2, 'OV'),
    ("OV.12", "Authority", 2, 'OV'),
    ("OV.13", "Social Status", 2, 'OV'),
]

async def ingest_onet_work_values(conn) -> int:
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
