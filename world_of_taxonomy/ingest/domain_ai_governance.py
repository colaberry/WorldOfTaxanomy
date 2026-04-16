"""AI Ethics and Governance Framework Types domain taxonomy ingester.

Classifies the governance, ethics, safety and regulatory compliance dimensions
of AI systems. Orthogonal to model type and deployment infrastructure.
Anchored in EU AI Act risk tiers, NIST AI RMF, ISO/IEC 42001, OECD AI Principles,
and emerging US Executive Order on Safe/Trustworthy AI.
Used by AI policy teams, risk officers, legal/compliance, and ESG reporting functions.

Code prefix: daigov_
System ID: domain_ai_governance
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
AI_GOVERNANCE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("daigov_riskclass", "AI System Risk Classification", 1, None),
    ("daigov_riskclass_unacceptable", "EU AI Act - Unacceptable risk (prohibited AI applications)", 2, "daigov_riskclass"),
    ("daigov_riskclass_high", "EU AI Act - High risk (critical infrastructure, employment, credit)", 2, "daigov_riskclass"),
    ("daigov_riskclass_limited", "EU AI Act - Limited risk (chatbots, emotion recognition)", 2, "daigov_riskclass"),
    ("daigov_riskclass_minimal", "EU AI Act - Minimal risk (spam filters, AI-enabled video games)", 2, "daigov_riskclass"),
    ("daigov_fairness", "Fairness, Bias and Non-Discrimination", 1, None),
    ("daigov_fairness_audit", "Algorithmic auditing and bias testing", 2, "daigov_fairness"),
    ("daigov_fairness_metrics", "Fairness metrics (demographic parity, equalized odds)", 2, "daigov_fairness"),
    ("daigov_fairness_remediation", "Bias mitigation and remediation techniques", 2, "daigov_fairness"),
    ("daigov_transparency", "Transparency and Explainability", 1, None),
    ("daigov_transparency_xai", "Explainable AI methods (SHAP, LIME, attention viz)", 2, "daigov_transparency"),
    ("daigov_transparency_disclosure", "AI disclosure and labeling requirements", 2, "daigov_transparency"),
    ("daigov_transparency_datasheet", "Model cards, datasheets and system cards", 2, "daigov_transparency"),
    ("daigov_safety", "AI Safety and Robustness", 1, None),
    ("daigov_safety_adversarial", "Adversarial robustness and red-teaming", 2, "daigov_safety"),
    ("daigov_safety_hallucination", "Hallucination detection and factuality controls", 2, "daigov_safety"),
    ("daigov_safety_alignment", "AI alignment and value alignment techniques", 2, "daigov_safety"),
    ("daigov_privacy", "Privacy and Data Protection in AI", 1, None),
    ("daigov_privacy_training", "Training data privacy (consent, PII scrubbing, synthetic)", 2, "daigov_privacy"),
    ("daigov_privacy_inference", "Inference-time privacy (differential privacy, federated)", 2, "daigov_privacy"),
    ("daigov_privacy_gdpr", "GDPR/CCPA compliance for automated decision-making", 2, "daigov_privacy"),
]

_DOMAIN_ROW = (
    "domain_ai_governance",
    "AI Ethics and Governance Framework Types",
    "AI governance, ethics, safety and regulatory compliance framework classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['5415', '5182', '9241']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_ai_governance(conn) -> int:
    """Ingest AI Ethics and Governance Framework Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_ai_governance'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_ai_governance",
        "AI Ethics and Governance Framework Types",
        "AI governance, ethics, safety and regulatory compliance framework classification",
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

    parent_codes = {parent for _, _, _, parent in AI_GOVERNANCE_NODES if parent is not None}

    rows = [
        (
            "domain_ai_governance",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in AI_GOVERNANCE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(AI_GOVERNANCE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_ai_governance'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_ai_governance'",
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
            [("naics_2022", code, "domain_ai_governance", "primary") for code in naics_codes],
        )

    return count
