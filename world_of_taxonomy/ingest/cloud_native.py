"""Ingest CNCF Cloud Native Landscape Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("cloud_native", "Cloud Native", "CNCF Cloud Native Landscape Categories", "2024", "Global", "CNCF")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Apache 2.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CN", "CNCF Landscape", 1, None),
    ("CN.01", "App Definition and Development", 2, 'CN'),
    ("CN.02", "Orchestration and Management", 2, 'CN'),
    ("CN.03", "Runtime", 2, 'CN'),
    ("CN.04", "Provisioning", 2, 'CN'),
    ("CN.05", "Observability and Analysis", 2, 'CN'),
    ("CN.06", "Serverless", 2, 'CN'),
    ("CN.07", "Service Mesh", 2, 'CN'),
    ("CN.08", "API Gateway", 2, 'CN'),
    ("CN.09", "Container Registry", 2, 'CN'),
    ("CN.10", "Security and Compliance", 2, 'CN'),
    ("CN.11", "Database (Cloud Native)", 2, 'CN'),
    ("CN.12", "Streaming and Messaging", 2, 'CN'),
    ("CN.13", "Continuous Integration/Delivery", 2, 'CN'),
    ("CN.14", "GitOps", 2, 'CN'),
]

async def ingest_cloud_native(conn) -> int:
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
