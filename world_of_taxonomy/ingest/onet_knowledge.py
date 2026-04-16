"""Ingest O*NET Knowledge Areas."""
from __future__ import annotations

_SYSTEM_ROW = ("onet_knowledge", "O*NET Knowledge", "O*NET Knowledge Areas", "2024", "United States", "DOL")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ONK", "O*NET Knowledge Areas", 1, None),
    ("ONK.01", "Business and Management", 2, 'ONK'),
    ("ONK.02", "Manufacturing and Production", 2, 'ONK'),
    ("ONK.03", "Engineering and Technology", 2, 'ONK'),
    ("ONK.04", "Mathematics and Science", 2, 'ONK'),
    ("ONK.05", "Health Services", 2, 'ONK'),
    ("ONK.06", "Education and Training", 2, 'ONK'),
    ("ONK.07", "Arts and Humanities", 2, 'ONK'),
    ("ONK.08", "Law and Public Safety", 2, 'ONK'),
    ("ONK.09", "Communications", 2, 'ONK'),
    ("ONK.10", "Transportation", 2, 'ONK'),
    ("ONK.11", "Sales and Marketing", 2, 'ONK'),
    ("ONK.12", "Customer and Personal Service", 2, 'ONK'),
    ("ONK.13", "Computers and Electronics", 2, 'ONK'),
]

async def ingest_onet_knowledge(conn) -> int:
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
