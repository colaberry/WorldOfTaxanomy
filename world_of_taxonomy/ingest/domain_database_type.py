"""Ingest Database Type Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_database_type", "Database Type", "Database Type Classification", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DB", "Database Types", 1, None),
    ("DB.01", "Relational (SQL)", 2, 'DB'),
    ("DB.02", "Document Store", 2, 'DB'),
    ("DB.03", "Key-Value Store", 2, 'DB'),
    ("DB.04", "Wide-Column Store", 2, 'DB'),
    ("DB.05", "Graph Database", 2, 'DB'),
    ("DB.06", "Time Series Database", 2, 'DB'),
    ("DB.07", "Vector Database", 2, 'DB'),
    ("DB.08", "In-Memory Database", 2, 'DB'),
    ("DB.09", "Object-Oriented Database", 2, 'DB'),
    ("DB.10", "NewSQL Database", 2, 'DB'),
    ("DB.11", "Multi-Model Database", 2, 'DB'),
    ("DB.12", "Spatial Database", 2, 'DB'),
    ("DB.13", "Embedded Database", 2, 'DB'),
    ("DB.14", "Ledger Database", 2, 'DB'),
    ("DB.15", "Search Engine Database", 2, 'DB'),
    ("DB.16", "Data Warehouse", 2, 'DB'),
]

async def ingest_domain_database_type(conn) -> int:
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
