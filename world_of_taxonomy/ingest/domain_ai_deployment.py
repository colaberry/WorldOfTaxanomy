"""AI Deployment Infrastructure Types domain taxonomy ingester.

Classifies how AI models are deployed and served in production.
Orthogonal to model type (domain_ai_data) and governance (domain_ai_governance).
Used by MLOps teams, cloud architects, hardware vendors, and AI product managers
when making infrastructure, latency, cost and data-residency trade-off decisions.
Aligns with NIST AI RMF infrastructure dimensions and cloud hyperscaler taxonomy.

Code prefix: daidep_
System ID: domain_ai_deployment
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
AI_DEPLOYMENT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("daidep_cloud", "Cloud-Based AI Deployment", 1, None),
    ("daidep_cloud_saas", "AI-as-a-Service (API access, OpenAI, Anthropic, Cohere)", 2, "daidep_cloud"),
    ("daidep_cloud_hosted", "Managed ML platforms (SageMaker, Vertex AI, Azure ML)", 2, "daidep_cloud"),
    ("daidep_cloud_gpu", "GPU cloud compute (A100/H100 clusters, spot instances)", 2, "daidep_cloud"),
    ("daidep_cloud_serverless", "Serverless inference (Lambda, Cloud Functions, modal)", 2, "daidep_cloud"),
    ("daidep_edge", "Edge and On-Device AI Deployment", 1, None),
    ("daidep_edge_mobile", "Mobile device inference (iOS Core ML, Android NNAPI)", 2, "daidep_edge"),
    ("daidep_edge_iot", "IoT edge inference (microcontrollers, TinyML, ONNX)", 2, "daidep_edge"),
    ("daidep_edge_gateway", "Edge gateway inference (Jetson, AWS Greengrass, Azure IoT Edge)", 2, "daidep_edge"),
    ("daidep_onprem", "On-Premise AI Infrastructure", 1, None),
    ("daidep_onprem_dcgpu", "On-premise GPU server clusters (DGX, custom HPC)", 2, "daidep_onprem"),
    ("daidep_onprem_aiappl", "Dedicated AI appliances (NVidia DGX Pod, Gaudi racks)", 2, "daidep_onprem"),
    ("daidep_onprem_private", "Private cloud AI (VMware, OpenStack with GPU passthrough)", 2, "daidep_onprem"),
    ("daidep_hybrid", "Hybrid and Multi-Cloud AI Deployment", 1, None),
    ("daidep_hybrid_federated", "Federated learning (train across distributed data without sharing)", 2, "daidep_hybrid"),
    ("daidep_hybrid_multicloud", "Multi-cloud inference routing (cost, latency, compliance)", 2, "daidep_hybrid"),
    ("daidep_hybrid_burst", "Burst-to-cloud from on-premise (reserved + spot mix)", 2, "daidep_hybrid"),
    ("daidep_pattern", "AI Serving and Inference Patterns", 1, None),
    ("daidep_pattern_batch", "Batch inference (overnight scoring pipelines)", 2, "daidep_pattern"),
    ("daidep_pattern_realtime", "Real-time inference (sub-100ms latency SLA)", 2, "daidep_pattern"),
    ("daidep_pattern_rag", "Retrieval-augmented generation (RAG) serving pattern", 2, "daidep_pattern"),
    ("daidep_pattern_ab", "A/B model serving and shadow deployment patterns", 2, "daidep_pattern"),
]

_DOMAIN_ROW = (
    "domain_ai_deployment",
    "AI Deployment Infrastructure Types",
    "AI inference deployment infrastructure classification: cloud, edge, on-premise, hybrid serving patterns",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['5415', '5182', '5191']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_ai_deployment(conn) -> int:
    """Ingest AI Deployment Infrastructure Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_ai_deployment'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_ai_deployment",
        "AI Deployment Infrastructure Types",
        "AI inference deployment infrastructure classification: cloud, edge, on-premise, hybrid serving patterns",
        "1.0",
        "Global",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in AI_DEPLOYMENT_NODES if parent is not None}

    rows = [
        (
            "domain_ai_deployment",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in AI_DEPLOYMENT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(AI_DEPLOYMENT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_ai_deployment'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_ai_deployment'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_ai_deployment", "primary") for code in naics_codes],
        )

    return count
