"""InsurTech domain taxonomy ingester.

Organizes insurance technology sector types aligned with
NAICS 5241 (Insurance carriers),
NAICS 5242 (Agencies, brokerages, and other insurance activities).

Code prefix: dit_
Categories: Digital Underwriting, Claims Automation,
Parametric Insurance, Peer-to-Peer Insurance, Insurance Analytics.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
INSURTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Digital Underwriting --
    ("dit_underwrite",            "Digital Underwriting",                                1, None),
    ("dit_underwrite_auto",       "Automated Risk Assessment and Scoring",              2, "dit_underwrite"),
    ("dit_underwrite_telematic",  "Telematics-Based Underwriting (auto, fleet)",        2, "dit_underwrite"),
    ("dit_underwrite_iot",        "IoT Sensor-Based Property Underwriting",             2, "dit_underwrite"),
    ("dit_underwrite_health",     "Digital Health Underwriting and Wearable Data",      2, "dit_underwrite"),
    ("dit_underwrite_instant",    "Instant and Embedded Insurance Issuance",            2, "dit_underwrite"),

    # -- Claims Automation --
    ("dit_claims",                "Claims Automation",                                   1, None),
    ("dit_claims_fnol",           "First Notice of Loss (FNOL) Digital Intake",         2, "dit_claims"),
    ("dit_claims_ai",             "AI-Powered Claims Assessment and Adjudication",      2, "dit_claims"),
    ("dit_claims_photo",          "Photo and Video Damage Estimation",                   2, "dit_claims"),
    ("dit_claims_fraud",          "Claims Fraud Detection and Prevention",               2, "dit_claims"),

    # -- Parametric Insurance --
    ("dit_parametric",            "Parametric Insurance",                                1, None),
    ("dit_parametric_weather",    "Weather-Triggered Parametric Coverage",               2, "dit_parametric"),
    ("dit_parametric_cat",        "Catastrophe Bond and Index-Based Solutions",          2, "dit_parametric"),
    ("dit_parametric_crop",       "Crop and Agricultural Parametric Products",           2, "dit_parametric"),
    ("dit_parametric_cyber",      "Cyber Event Parametric Insurance",                    2, "dit_parametric"),

    # -- Peer-to-Peer Insurance --
    ("dit_p2p",                   "Peer-to-Peer Insurance",                              1, None),
    ("dit_p2p_mutual",            "Digital Mutual Aid and Group Insurance",              2, "dit_p2p"),
    ("dit_p2p_micro",             "Microinsurance and On-Demand Coverage",               2, "dit_p2p"),
    ("dit_p2p_community",         "Community-Based Risk Pooling Platforms",              2, "dit_p2p"),
    ("dit_p2p_dao",               "Decentralized Insurance (DAO-based) Models",         2, "dit_p2p"),

    # -- Insurance Analytics --
    ("dit_analytics",             "Insurance Analytics",                                  1, None),
    ("dit_analytics_actuarial",   "AI-Enhanced Actuarial Modeling",                      2, "dit_analytics"),
    ("dit_analytics_portfolio",   "Portfolio Risk Analytics and Optimization",           2, "dit_analytics"),
    ("dit_analytics_customer",    "Customer Segmentation and Retention Analytics",       2, "dit_analytics"),
    ("dit_analytics_pricing",     "Dynamic Pricing and Rate Optimization",               2, "dit_analytics"),
]

_DOMAIN_ROW = (
    "domain_insurtech",
    "InsurTech Types",
    "Insurance technology types covering digital underwriting, claims automation, "
    "parametric insurance, peer-to-peer insurance, and insurance analytics taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5241 (Insurance carriers), 5242 (Agencies/brokerages)
_NAICS_PREFIXES = ["5241", "5242"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific InsurTech types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_insurtech(conn) -> int:
    """Ingest InsurTech domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_insurtech'), and links NAICS 5241/5242 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_insurtech",
        "InsurTech Types",
        "Insurance technology types covering digital underwriting, claims automation, "
        "parametric insurance, peer-to-peer insurance, and insurance analytics taxonomy",
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

    parent_codes = {parent for _, _, _, parent in INSURTECH_NODES if parent is not None}

    rows = [
        (
            "domain_insurtech",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in INSURTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(INSURTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_insurtech'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_insurtech'",
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
            [("naics_2022", code, "domain_insurtech", "primary") for code in naics_codes],
        )

    return count
