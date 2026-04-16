"""Ingest Model Registry Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_model_registry", "Model Registry", "Model Registry Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MR", "Model Registry Types", 1, None),
    ("MR.01", "Model Version Control", 2, 'MR'),
    ("MR.02", "Model Metadata Store", 2, 'MR'),
    ("MR.03", "Model Artifact Storage", 2, 'MR'),
    ("MR.04", "Model Stage Promotion", 2, 'MR'),
    ("MR.05", "Model Approval Workflow", 2, 'MR'),
    ("MR.06", "Model Lineage Tracking", 2, 'MR'),
    ("MR.07", "Model Performance Baseline", 2, 'MR'),
    ("MR.08", "Model Deployment Trigger", 2, 'MR'),
    ("MR.09", "Model Rollback", 2, 'MR'),
    ("MR.10", "Model Compliance Check", 2, 'MR'),
    ("MR.11", "Model Card Generation", 2, 'MR'),
    ("MR.12", "Model Deprecation", 2, 'MR'),
    ("MR.13", "Multi-Framework Registry", 2, 'MR'),
]

async def ingest_domain_model_registry(conn) -> int:
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
