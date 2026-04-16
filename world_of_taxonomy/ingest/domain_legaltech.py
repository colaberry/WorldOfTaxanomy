"""LegalTech domain taxonomy ingester.

Organizes legal technology sector types aligned with
NAICS 5411 (Legal services),
NAICS 5112 (Software publishers).

Code prefix: dlt_
Categories: Contract Automation, Legal Research AI,
E-Discovery, Compliance Automation, Court Technology.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
LEGALTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Contract Automation --
    ("dlt_contract",            "Contract Automation",                                  1, None),
    ("dlt_contract_lifecycle",  "Contract Lifecycle Management (CLM) Platforms",       2, "dlt_contract"),
    ("dlt_contract_gen",        "Automated Contract Drafting and Generation",          2, "dlt_contract"),
    ("dlt_contract_review",     "AI-Powered Contract Review and Analysis",             2, "dlt_contract"),
    ("dlt_contract_esign",      "Electronic Signature and Execution Platforms",        2, "dlt_contract"),

    # -- Legal Research AI --
    ("dlt_research",            "Legal Research AI",                                    1, None),
    ("dlt_research_case",       "Case Law Search and Analysis Engines",                2, "dlt_research"),
    ("dlt_research_statute",    "Statute and Regulation Retrieval Platforms",           2, "dlt_research"),
    ("dlt_research_predict",    "Litigation Outcome Prediction and Analytics",         2, "dlt_research"),
    ("dlt_research_cite",       "Citation Analysis and Legal Brief Automation",        2, "dlt_research"),

    # -- E-Discovery --
    ("dlt_ediscovery",          "E-Discovery",                                          1, None),
    ("dlt_ediscovery_collect",  "Data Collection and Forensic Preservation",           2, "dlt_ediscovery"),
    ("dlt_ediscovery_process",  "Document Processing and De-Duplication",              2, "dlt_ediscovery"),
    ("dlt_ediscovery_review",   "Technology-Assisted Review (TAR) and Predictive Coding", 2, "dlt_ediscovery"),
    ("dlt_ediscovery_produce",  "Document Production and Redaction Tools",             2, "dlt_ediscovery"),
    ("dlt_ediscovery_forensic", "Digital Forensics and Data Recovery",                 2, "dlt_ediscovery"),

    # -- Compliance Automation --
    ("dlt_compliance",          "Compliance Automation",                                1, None),
    ("dlt_compliance_monitor",  "Regulatory Change Monitoring and Alerts",             2, "dlt_compliance"),
    ("dlt_compliance_policy",   "Policy Management and Distribution Platforms",        2, "dlt_compliance"),
    ("dlt_compliance_audit",    "Compliance Audit Trail and Reporting Tools",          2, "dlt_compliance"),
    ("dlt_compliance_privacy",  "Data Privacy and GDPR Compliance Automation",         2, "dlt_compliance"),

    # -- Court Technology --
    ("dlt_court",               "Court Technology",                                     1, None),
    ("dlt_court_filing",        "Electronic Court Filing (e-Filing) Systems",          2, "dlt_court"),
    ("dlt_court_virtual",       "Virtual Hearing and Online Dispute Resolution",       2, "dlt_court"),
    ("dlt_court_docket",        "Docket Management and Case Tracking",                 2, "dlt_court"),
    ("dlt_court_jury",          "Jury Selection and Management Technology",             2, "dlt_court"),
]

_DOMAIN_ROW = (
    "domain_legaltech",
    "LegalTech Types",
    "Legal technology types covering contract automation, legal research AI, "
    "e-discovery, compliance automation, and court technology taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5411 (Legal services), 5112 (Software publishers)
_NAICS_PREFIXES = ["5411", "5112"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific LegalTech types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_legaltech(conn) -> int:
    """Ingest LegalTech domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_legaltech'), and links NAICS 5411/5112 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_legaltech",
        "LegalTech Types",
        "Legal technology types covering contract automation, legal research AI, "
        "e-discovery, compliance automation, and court technology taxonomy",
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

    parent_codes = {parent for _, _, _, parent in LEGALTECH_NODES if parent is not None}

    rows = [
        (
            "domain_legaltech",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in LEGALTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(LEGALTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_legaltech'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_legaltech'",
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
            [("naics_2022", code, "domain_legaltech", "primary") for code in naics_codes],
        )

    return count
