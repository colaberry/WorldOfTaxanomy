"""Ingest Feature Store Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_feature_store", "Feature Store", "Feature Store Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FS", "Feature Store Types", 1, None),
    ("FS.01", "Online Feature Store", 2, 'FS'),
    ("FS.02", "Offline Feature Store", 2, 'FS'),
    ("FS.03", "Feature Registry", 2, 'FS'),
    ("FS.04", "Feature Transformation", 2, 'FS'),
    ("FS.05", "Feature Serving", 2, 'FS'),
    ("FS.06", "Point-in-Time Join", 2, 'FS'),
    ("FS.07", "Feature Freshness SLA", 2, 'FS'),
    ("FS.08", "Feature Lineage", 2, 'FS'),
    ("FS.09", "Feature Monitoring", 2, 'FS'),
    ("FS.10", "Streaming Feature", 2, 'FS'),
    ("FS.11", "Batch Feature", 2, 'FS'),
    ("FS.12", "Feature Discovery", 2, 'FS'),
    ("FS.13", "Feature Access Control", 2, 'FS'),
    ("FS.14", "Feature Versioning", 2, 'FS'),
]

async def ingest_domain_feature_store(conn) -> int:
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
