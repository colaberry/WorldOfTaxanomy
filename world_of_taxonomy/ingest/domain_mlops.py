"""Ingest MLOps Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_mlops", "MLOps", "MLOps Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ML", "MLOps Types", 1, None),
    ("ML.01", "Model Training Pipeline", 2, 'ML'),
    ("ML.02", "Model Serving Infrastructure", 2, 'ML'),
    ("ML.03", "Experiment Tracking", 2, 'ML'),
    ("ML.04", "Model Monitoring", 2, 'ML'),
    ("ML.05", "Data Drift Detection", 2, 'ML'),
    ("ML.06", "Model Drift Detection", 2, 'ML'),
    ("ML.07", "A/B Testing (Model)", 2, 'ML'),
    ("ML.08", "Shadow Deployment", 2, 'ML'),
    ("ML.09", "Model Explainability", 2, 'ML'),
    ("ML.10", "Feature Pipeline", 2, 'ML'),
    ("ML.11", "Training Data Management", 2, 'ML'),
    ("ML.12", "Model Compression", 2, 'ML'),
    ("ML.13", "Edge ML Deployment", 2, 'ML'),
    ("ML.14", "LLMOps", 2, 'ML'),
    ("ML.15", "ML Platform Architecture", 2, 'ML'),
    ("ML.16", "Responsible AI Pipeline", 2, 'ML'),
]

async def ingest_domain_mlops(conn) -> int:
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
