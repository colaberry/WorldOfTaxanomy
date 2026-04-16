"""Ingest Data Lakehouse Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_data_lakehouse", "Data Lakehouse", "Data Lakehouse Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DL", "Data Lakehouse Types", 1, None),
    ("DL.01", "Delta Lake", 2, 'DL'),
    ("DL.02", "Apache Iceberg", 2, 'DL'),
    ("DL.03", "Apache Hudi", 2, 'DL'),
    ("DL.04", "Medallion Architecture (Bronze)", 2, 'DL'),
    ("DL.05", "Medallion Architecture (Silver)", 2, 'DL'),
    ("DL.06", "Medallion Architecture (Gold)", 2, 'DL'),
    ("DL.07", "Schema Evolution", 2, 'DL'),
    ("DL.08", "Time Travel (Data Versioning)", 2, 'DL'),
    ("DL.09", "Data Compaction", 2, 'DL'),
    ("DL.10", "Partition Pruning", 2, 'DL'),
    ("DL.11", "Zero-Copy Clone", 2, 'DL'),
    ("DL.12", "Unity Catalog", 2, 'DL'),
    ("DL.13", "Open Table Format", 2, 'DL'),
    ("DL.14", "Lakehouse Federation", 2, 'DL'),
]

async def ingest_domain_data_lakehouse(conn) -> int:
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
