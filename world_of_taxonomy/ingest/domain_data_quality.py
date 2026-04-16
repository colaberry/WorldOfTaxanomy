"""Ingest Data Quality Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_data_quality", "Data Quality", "Data Quality Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DQ", "Data Quality Types", 1, None),
    ("DQ.01", "Completeness Check", 2, 'DQ'),
    ("DQ.02", "Accuracy Validation", 2, 'DQ'),
    ("DQ.03", "Consistency Check", 2, 'DQ'),
    ("DQ.04", "Timeliness Check", 2, 'DQ'),
    ("DQ.05", "Uniqueness Validation", 2, 'DQ'),
    ("DQ.06", "Referential Integrity", 2, 'DQ'),
    ("DQ.07", "Schema Validation", 2, 'DQ'),
    ("DQ.08", "Statistical Profiling", 2, 'DQ'),
    ("DQ.09", "Anomaly Detection", 2, 'DQ'),
    ("DQ.10", "Data Reconciliation", 2, 'DQ'),
    ("DQ.11", "Great Expectations Framework", 2, 'DQ'),
    ("DQ.12", "dbt Tests", 2, 'DQ'),
    ("DQ.13", "Data Quality SLA", 2, 'DQ'),
    ("DQ.14", "Data Quality Dashboard", 2, 'DQ'),
    ("DQ.15", "Root Cause Analysis", 2, 'DQ'),
]

async def ingest_domain_data_quality(conn) -> int:
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
