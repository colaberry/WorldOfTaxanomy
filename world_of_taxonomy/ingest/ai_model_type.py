"""Ingest Machine Learning Model Type Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("ai_model_type", "AI Model Types", "Machine Learning Model Type Categories", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AI", "AI Model Types", 1, None),
    ("AI.01", "Supervised: Classification", 2, 'AI'),
    ("AI.02", "Supervised: Regression", 2, 'AI'),
    ("AI.03", "Unsupervised: Clustering", 2, 'AI'),
    ("AI.04", "Unsupervised: Dimensionality Reduction", 2, 'AI'),
    ("AI.05", "Reinforcement Learning", 2, 'AI'),
    ("AI.06", "Deep Learning: CNN", 2, 'AI'),
    ("AI.07", "Deep Learning: RNN/LSTM", 2, 'AI'),
    ("AI.08", "Deep Learning: Transformer", 2, 'AI'),
    ("AI.09", "Generative: GAN", 2, 'AI'),
    ("AI.10", "Generative: VAE", 2, 'AI'),
    ("AI.11", "Generative: Diffusion Model", 2, 'AI'),
    ("AI.12", "Foundation Model (LLM)", 2, 'AI'),
    ("AI.13", "Graph Neural Network", 2, 'AI'),
    ("AI.14", "Multi-Modal Model", 2, 'AI'),
    ("AI.15", "Neuro-Symbolic", 2, 'AI'),
    ("AI.16", "Federated Learning", 2, 'AI'),
]

async def ingest_ai_model_type(conn) -> int:
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
