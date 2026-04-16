"""Ingest WorldSkills Competition Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("worldskills", "WorldSkills", "WorldSkills Competition Categories", "2024", "Global", "WorldSkills International")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WS", "WorldSkills Categories", 1, None),
    ("WS.01", "Construction and Building Technology", 2, 'WS'),
    ("WS.02", "Creative Arts and Fashion", 2, 'WS'),
    ("WS.03", "Information and Communication Technology", 2, 'WS'),
    ("WS.04", "Manufacturing and Engineering Technology", 2, 'WS'),
    ("WS.05", "Social and Personal Services", 2, 'WS'),
    ("WS.06", "Transportation and Logistics", 2, 'WS'),
    ("WS.07", "Landscape and Agriculture", 2, 'WS'),
    ("WS.08", "Welding and Joining", 2, 'WS'),
    ("WS.09", "Electrical Installations", 2, 'WS'),
    ("WS.10", "Mechatronics and Robotics", 2, 'WS'),
    ("WS.11", "Web Technologies", 2, 'WS'),
    ("WS.12", "Cooking and Hospitality", 2, 'WS'),
    ("WS.13", "Health and Social Care", 2, 'WS'),
]

async def ingest_worldskills(conn) -> int:
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
