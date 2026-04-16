"""Ingest Freight Class Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_freight_class", "Freight Class", "Freight Class Types", "1.0", "United States", "NMFTA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FC", "Freight Class Types", 1, None),
    ("FC.01", "Class 50 (Heaviest/Densest)", 2, 'FC'),
    ("FC.02", "Class 55", 2, 'FC'),
    ("FC.03", "Class 60", 2, 'FC'),
    ("FC.04", "Class 65", 2, 'FC'),
    ("FC.05", "Class 70", 2, 'FC'),
    ("FC.06", "Class 77.5", 2, 'FC'),
    ("FC.07", "Class 85", 2, 'FC'),
    ("FC.08", "Class 92.5", 2, 'FC'),
    ("FC.09", "Class 100", 2, 'FC'),
    ("FC.10", "Class 110", 2, 'FC'),
    ("FC.11", "Class 125", 2, 'FC'),
    ("FC.12", "Class 150", 2, 'FC'),
    ("FC.13", "Class 175", 2, 'FC'),
    ("FC.14", "Class 200", 2, 'FC'),
    ("FC.15", "Class 250", 2, 'FC'),
    ("FC.16", "Class 300", 2, 'FC'),
    ("FC.17", "Class 400", 2, 'FC'),
    ("FC.18", "Class 500 (Lightest/Least Dense)", 2, 'FC'),
]

async def ingest_domain_freight_class(conn) -> int:
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
