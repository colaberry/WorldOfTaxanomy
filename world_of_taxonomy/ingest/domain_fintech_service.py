"""FinTech Services domain taxonomy ingester.

FinTech taxonomy organizes financial technology service models and segments:
  Digital Payments  (dft_payments*) - mobile wallet, P2P, BNPL, merchant acquiring
  Neobanking        (dft_neobank*)  - digital-only bank, challenger, BaaS
  Lending Platforms (dft_lending*)  - marketplace, BNPL, invoice, micro-lending
  WealthTech        (dft_wealth*)   - robo-advisor, fractional, social trading
  RegTech           (dft_regtech*)  - KYC/AML, compliance, risk monitoring

Source: BIS (Bank for International Settlements) and CFPB fintech frameworks.
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FINTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Digital Payments category --
    ("dft_payments",            "Digital Payments",                                     1, None),
    ("dft_payments_wallet",     "Mobile Wallet and Contactless (Apple Pay, GPay)",      2, "dft_payments"),
    ("dft_payments_p2p",        "Peer-to-Peer Transfer (Venmo, Zelle, Cash App)",      2, "dft_payments"),
    ("dft_payments_bnpl",       "Buy Now Pay Later (Klarna, Afterpay, Affirm)",        2, "dft_payments"),
    ("dft_payments_merchant",   "Merchant Acquiring and POS (Square, Stripe, Adyen)",  2, "dft_payments"),
    ("dft_payments_cross",      "Cross-Border Payment (Wise, Remitly, remittance)",    2, "dft_payments"),

    # -- Neobanking category --
    ("dft_neobank",             "Neobanking and Digital Banking",                       1, None),
    ("dft_neobank_consumer",    "Consumer Neobank (Chime, Revolut, N26)",              2, "dft_neobank"),
    ("dft_neobank_business",    "Business Neobank (Mercury, Brex, Novo)",              2, "dft_neobank"),
    ("dft_neobank_baas",        "Banking-as-a-Service Platform (Synapse, Unit)",       2, "dft_neobank"),
    ("dft_neobank_embedded",    "Embedded Finance (non-bank product, white-label)",    2, "dft_neobank"),

    # -- Lending Platforms category --
    ("dft_lending",             "Lending Platforms",                                    1, None),
    ("dft_lending_marketplace", "Marketplace Lending (LendingClub, Prosper, P2P)",     2, "dft_lending"),
    ("dft_lending_invoice",     "Invoice Factoring and Supply Chain Finance",           2, "dft_lending"),
    ("dft_lending_micro",       "Micro-Lending and Nano-Credit (emerging markets)",    2, "dft_lending"),
    ("dft_lending_mortgage",    "Digital Mortgage and Home Lending (Rocket, Better)",   2, "dft_lending"),
    ("dft_lending_student",     "Student Loan and Education Finance (SoFi, Earnest)",  2, "dft_lending"),

    # -- WealthTech category --
    ("dft_wealth",              "WealthTech and Investment Platforms",                  1, None),
    ("dft_wealth_robo",         "Robo-Advisor (Betterment, Wealthfront, automated)",   2, "dft_wealth"),
    ("dft_wealth_fractional",   "Fractional Investing (stocks, real estate, art)",     2, "dft_wealth"),
    ("dft_wealth_social",       "Social and Copy Trading (eToro, Public.com)",          2, "dft_wealth"),
    ("dft_wealth_crypto",       "Crypto Brokerage and Exchange (Coinbase, Kraken)",    2, "dft_wealth"),

    # -- RegTech Services category --
    ("dft_regtech",             "RegTech Services",                                    1, None),
    ("dft_regtech_kyc",         "KYC and Identity Verification (Jumio, Onfido)",       2, "dft_regtech"),
    ("dft_regtech_aml",         "AML and Transaction Monitoring (Chainalysis, Feedzai)",2, "dft_regtech"),
    ("dft_regtech_compliance",  "Regulatory Compliance Platform (ComplyAdvantage)",    2, "dft_regtech"),
    ("dft_regtech_risk",        "Risk Analytics and Scoring (credit, fraud, cyber)",   2, "dft_regtech"),
]

_DOMAIN_ROW = (
    "domain_fintech_service",
    "FinTech Services Types",
    "Digital payments, neobanking, lending, WealthTech and RegTech taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["5221", "5222", "5231"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific fintech types."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_fintech_service(conn) -> int:
    """Ingest FinTech Services domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_fintech_service'), and links NAICS 5221/5222/5231 nodes
    via node_taxonomy_link.

    Returns total fintech service node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_fintech_service",
        "FinTech Services Types",
        "Digital payments, neobanking, lending, WealthTech and RegTech taxonomy",
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

    parent_codes = {parent for _, _, _, parent in FINTECH_NODES if parent is not None}

    rows = [
        (
            "domain_fintech_service",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in FINTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FINTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_fintech_service'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_fintech_service'",
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
            [("naics_2022", code, "domain_fintech_service", "primary") for code in naics_codes],
        )

    return count
