"""Insurance Risk domain taxonomy ingester.

Organizes insurance risk categories aligned with NAICS 5241 (Insurance carriers).

Code prefix: dir_
Categories: Natural Catastrophe, Liability, Financial, Operational, Cyber Risk.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
INSURANCE_RISK_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Natural Catastrophe --
    ("dir_natcat",            "Natural Catastrophe Risk",                              1, None),
    ("dir_natcat_hurricane",  "Hurricane and Windstorm Risk (tropical cyclone, tornado)", 2, "dir_natcat"),
    ("dir_natcat_earthquake", "Earthquake Risk (seismic, liquefaction, tsunami)",      2, "dir_natcat"),
    ("dir_natcat_flood",      "Flood Risk (riverine, coastal, pluvial, flash flood)",  2, "dir_natcat"),
    ("dir_natcat_wildfire",   "Wildfire Risk (wildland-urban interface, prescribed)",   2, "dir_natcat"),

    # -- Liability Risk --
    ("dir_liab",              "Liability Risk",                                        1, None),
    ("dir_liab_product",      "Product Liability Risk (manufacturing defect, design)", 2, "dir_liab"),
    ("dir_liab_prof",         "Professional Liability Risk (malpractice, negligence)", 2, "dir_liab"),
    ("dir_liab_enviro",       "Environmental Liability Risk (pollution, remediation)", 2, "dir_liab"),
    ("dir_liab_employer",     "Employer Liability Risk (workplace injury, harassment)", 2, "dir_liab"),

    # -- Financial Risk --
    ("dir_fin",               "Financial Risk",                                        1, None),
    ("dir_fin_credit",        "Credit Risk (default, counterparty, concentration)",    2, "dir_fin"),
    ("dir_fin_market",        "Market Risk (interest rate, equity, currency, commodity)", 2, "dir_fin"),
    ("dir_fin_liquidity",     "Liquidity Risk (funding, asset-liability mismatch)",    2, "dir_fin"),
    ("dir_fin_reserve",       "Reserve Risk (loss reserve inadequacy, IBNR)",          2, "dir_fin"),

    # -- Operational Risk --
    ("dir_ops",               "Operational Risk",                                      1, None),
    ("dir_ops_fraud",         "Fraud Risk (claims fraud, agent fraud, identity theft)", 2, "dir_ops"),
    ("dir_ops_model",         "Model Risk (pricing errors, catastrophe model failure)", 2, "dir_ops"),
    ("dir_ops_regulatory",    "Regulatory Risk (compliance failure, sanctions, fines)", 2, "dir_ops"),
    ("dir_ops_vendor",        "Third-Party and Vendor Risk (outsourcing, supply chain)", 2, "dir_ops"),

    # -- Cyber Risk --
    ("dir_cyber",             "Cyber Risk",                                            1, None),
    ("dir_cyber_breach",      "Data Breach Risk (PII exposure, notification costs)",   2, "dir_cyber"),
    ("dir_cyber_ransom",      "Ransomware and Extortion Risk (encryption, exfiltration)", 2, "dir_cyber"),
    ("dir_cyber_bec",         "Business Email Compromise Risk (social engineering)",   2, "dir_cyber"),
    ("dir_cyber_cloud",       "Cloud and Infrastructure Risk (misconfiguration, outage)", 2, "dir_cyber"),
]

_DOMAIN_ROW = (
    "domain_insurance_risk",
    "Insurance Risk Types",
    "Natural catastrophe, liability, financial, operational, "
    "and cyber risk category taxonomy for insurance",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefix: 5241 (Insurance carriers)
_NAICS_PREFIXES = ["5241"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific insurance risk types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_insurance_risk(conn) -> int:
    """Ingest Insurance Risk domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_insurance_risk'), and links NAICS 5241 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_insurance_risk",
        "Insurance Risk Types",
        "Natural catastrophe, liability, financial, operational, "
        "and cyber risk category taxonomy for insurance",
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

    parent_codes = {parent for _, _, _, parent in INSURANCE_RISK_NODES if parent is not None}

    rows = [
        (
            "domain_insurance_risk",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in INSURANCE_RISK_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(INSURANCE_RISK_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_insurance_risk'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_insurance_risk'",
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
            [("naics_2022", code, "domain_insurance_risk", "primary") for code in naics_codes],
        )

    return count
