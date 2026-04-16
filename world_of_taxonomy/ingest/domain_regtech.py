"""RegTech domain taxonomy ingester.

Organizes regulatory technology sector types aligned with
NAICS 5221 (Depository credit intermediation),
NAICS 5415 (Computer systems design and related services).

Code prefix: drt_
Categories: KYC/AML Automation, Regulatory Reporting,
Risk Monitoring, Compliance Training, Sanctions Screening.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
REGTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- KYC/AML Automation --
    ("drt_kyc",               "KYC/AML Automation",                                    1, None),
    ("drt_kyc_identity",      "Digital Identity Verification and eKYC",               2, "drt_kyc"),
    ("drt_kyc_transaction",   "Transaction Monitoring and Suspicious Activity",       2, "drt_kyc"),
    ("drt_kyc_beneficial",    "Beneficial Ownership Identification",                   2, "drt_kyc"),
    ("drt_kyc_onboarding",    "Customer Onboarding Automation",                        2, "drt_kyc"),
    ("drt_kyc_pep",           "PEP and Adverse Media Screening",                      2, "drt_kyc"),

    # -- Regulatory Reporting --
    ("drt_reporting",         "Regulatory Reporting",                                   1, None),
    ("drt_reporting_auto",    "Automated Regulatory Filing and Submission",            2, "drt_reporting"),
    ("drt_reporting_data",    "Regulatory Data Aggregation and Validation",            2, "drt_reporting"),
    ("drt_reporting_dash",    "Compliance Dashboard and Visualization",                2, "drt_reporting"),
    ("drt_reporting_xbrl",    "XBRL and Structured Reporting Standards",               2, "drt_reporting"),

    # -- Risk Monitoring --
    ("drt_risk",              "Risk Monitoring",                                        1, None),
    ("drt_risk_credit",       "Credit Risk Monitoring and Early Warning Systems",      2, "drt_risk"),
    ("drt_risk_market",       "Market Risk and Volatility Monitoring",                 2, "drt_risk"),
    ("drt_risk_operational",  "Operational Risk Event Detection",                       2, "drt_risk"),
    ("drt_risk_cyber",        "Cyber Risk Assessment and Monitoring",                   2, "drt_risk"),
    ("drt_risk_climate",      "Climate and ESG Risk Monitoring",                        2, "drt_risk"),

    # -- Compliance Training --
    ("drt_training",          "Compliance Training",                                    1, None),
    ("drt_training_elearn",   "E-Learning and Compliance Course Platforms",            2, "drt_training"),
    ("drt_training_cert",     "Certification Tracking and Renewal Management",         2, "drt_training"),
    ("drt_training_sim",      "Compliance Simulation and Scenario Testing",            2, "drt_training"),
    ("drt_training_assess",   "Knowledge Assessment and Attestation Tools",            2, "drt_training"),

    # -- Sanctions Screening --
    ("drt_sanctions",         "Sanctions Screening",                                    1, None),
    ("drt_sanctions_list",    "Sanctions List Screening and Matching",                  2, "drt_sanctions"),
    ("drt_sanctions_payment", "Payment and Trade Finance Screening",                   2, "drt_sanctions"),
    ("drt_sanctions_vessel",  "Vessel and Maritime Sanctions Tracking",                 2, "drt_sanctions"),
    ("drt_sanctions_crypto",  "Cryptocurrency and Blockchain Sanctions Monitoring",    2, "drt_sanctions"),
]

_DOMAIN_ROW = (
    "domain_regtech",
    "RegTech Types",
    "Regulatory technology types covering KYC/AML automation, regulatory reporting, "
    "risk monitoring, compliance training, and sanctions screening taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5221 (Depository credit), 5415 (Computer systems design)
_NAICS_PREFIXES = ["5221", "5415"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific RegTech types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_regtech(conn) -> int:
    """Ingest RegTech domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_regtech'), and links NAICS 5221/5415 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_regtech",
        "RegTech Types",
        "Regulatory technology types covering KYC/AML automation, regulatory reporting, "
        "risk monitoring, compliance training, and sanctions screening taxonomy",
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

    parent_codes = {parent for _, _, _, parent in REGTECH_NODES if parent is not None}

    rows = [
        (
            "domain_regtech",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in REGTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REGTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_regtech'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_regtech'",
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
            [("naics_2022", code, "domain_regtech", "primary") for code in naics_codes],
        )

    return count
