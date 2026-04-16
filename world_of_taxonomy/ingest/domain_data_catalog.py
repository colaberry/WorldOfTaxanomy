"""Ingest Data Catalog Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_data_catalog", "Data Catalog", "Data Catalog Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DC", "Data Catalog Types", 1, None),
    ("DC.01", "Metadata Catalog", 2, 'DC'),
    ("DC.02", "Business Glossary", 2, 'DC'),
    ("DC.03", "Data Dictionary", 2, 'DC'),
    ("DC.04", "Schema Registry", 2, 'DC'),
    ("DC.05", "Data Classification Tag", 2, 'DC'),
    ("DC.06", "Data Owner Assignment", 2, 'DC'),
    ("DC.07", "Data Usage Metric", 2, 'DC'),
    ("DC.08", "Data Freshness Indicator", 2, 'DC'),
    ("DC.09", "Data Profiling", 2, 'DC'),
    ("DC.10", "Data Search and Discovery", 2, 'DC'),
    ("DC.11", "Data Collaboration", 2, 'DC'),
    ("DC.12", "Data Lineage Visualization", 2, 'DC'),
    ("DC.13", "Automated Metadata Ingestion", 2, 'DC'),
    ("DC.14", "Data Trust Score", 2, 'DC'),
]

async def ingest_domain_data_catalog(conn) -> int:
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
