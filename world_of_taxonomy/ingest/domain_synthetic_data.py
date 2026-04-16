"""Ingest Synthetic Data Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_synthetic_data", "Synthetic Data", "Synthetic Data Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SD", "Synthetic Data Types", 1, None),
    ("SD.01", "Tabular Synthetic Data", 2, 'SD'),
    ("SD.02", "Text Synthetic Data (LLM)", 2, 'SD'),
    ("SD.03", "Image Synthetic Data (GAN)", 2, 'SD'),
    ("SD.04", "Time Series Synthetic", 2, 'SD'),
    ("SD.05", "Graph Synthetic Data", 2, 'SD'),
    ("SD.06", "Differential Privacy Synthetic", 2, 'SD'),
    ("SD.07", "Rule-Based Synthetic", 2, 'SD'),
    ("SD.08", "Statistical Synthetic", 2, 'SD'),
    ("SD.09", "Simulation-Based Synthetic", 2, 'SD'),
    ("SD.10", "Augmentation Technique", 2, 'SD'),
    ("SD.11", "Synthetic Data Quality Metric", 2, 'SD'),
    ("SD.12", "Privacy Utility Tradeoff", 2, 'SD'),
    ("SD.13", "Synthetic Test Data", 2, 'SD'),
    ("SD.14", "Digital Twin Synthetic", 2, 'SD'),
]

async def ingest_domain_synthetic_data(conn) -> int:
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
