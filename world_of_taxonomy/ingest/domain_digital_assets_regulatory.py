"""Digital Assets Regulatory Framework Types domain taxonomy ingester.

Classifies the regulatory frameworks governing digital assets, cryptocurrencies,
and blockchain-based financial instruments.
Orthogonal to asset type (domain_digital_assets). Used by compliance officers,
legal teams, and regulatory affairs at crypto exchanges, DeFi protocols,
and traditional finance firms entering digital assets.
Key frameworks: EU MiCA, US Howey test / SEC/CFTC jurisdiction, FATF Travel Rule.

Code prefix: ddareg_
System ID: domain_digital_assets_regulatory
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
DIGITAL_ASSETS_REGULATORY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ddareg_securities", "Securities and Investment Regulation", 1, None),
    ("ddareg_securities_howey", "US Howey test - security token vs. commodity determination", 2, "ddareg_securities"),
    ("ddareg_securities_mica", "EU MiCA Regulation (2024) - e-money tokens, ARTs, utility tokens", 2, "ddareg_securities"),
    ("ddareg_securities_prospectus", "Security token offerings (STOs) - prospectus and disclosure", 2, "ddareg_securities"),
    ("ddareg_aml", "AML / CFT and Travel Rule Compliance", 1, None),
    ("ddareg_aml_fatf", "FATF Recommendation 16 (Travel Rule) for VASPs", 2, "ddareg_aml"),
    ("ddareg_aml_kyc", "KYC / CDD requirements for crypto on/off-ramps", 2, "ddareg_aml"),
    ("ddareg_aml_sanctioned", "Sanctions screening (OFAC, EU, UN SDN list compliance)", 2, "ddareg_aml"),
    ("ddareg_exchange", "Exchange and Custodian Licensing", 1, None),
    ("ddareg_exchange_us", "US crypto exchange licensing (BitLicense, MSB registration, MTL)", 2, "ddareg_exchange"),
    ("ddareg_exchange_eu", "EU CASP license under MiCA (authorization requirements)", 2, "ddareg_exchange"),
    ("ddareg_exchange_apac", "APAC digital asset exchange licenses (MAS, SFC Hong Kong, FSA Japan)", 2, "ddareg_exchange"),
    ("ddareg_stablecoin", "Stablecoin and Payment Token Regulation", 1, None),
    ("ddareg_stablecoin_emi", "E-money token issuance under MiCA / EMD2", 2, "ddareg_stablecoin"),
    ("ddareg_stablecoin_us", "US stablecoin bill framework (Fed, OCC, state money transmission)", 2, "ddareg_stablecoin"),
    ("ddareg_cbdc", "Central Bank Digital Currency (CBDC) Frameworks", 1, None),
    ("ddareg_cbdc_retail", "Retail CBDC design and pilot programmes (digital dollar, e-CNY, e-EUR)", 2, "ddareg_cbdc"),
    ("ddareg_cbdc_wholesale", "Wholesale CBDC for interbank settlement (Banque de France, BIS mBridge)", 2, "ddareg_cbdc"),
]

_DOMAIN_ROW = (
    "domain_digital_assets_regulatory",
    "Digital Assets Regulatory Framework Types",
    "Crypto and digital asset regulatory classification: securities law, AML/KYC, exchange licensing, CBDC",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['5221', '5223', '5231']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_digital_assets_regulatory(conn) -> int:
    """Ingest Digital Assets Regulatory Framework Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_digital_assets_regulatory'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_digital_assets_regulatory",
        "Digital Assets Regulatory Framework Types",
        "Crypto and digital asset regulatory classification: securities law, AML/KYC, exchange licensing, CBDC",
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

    parent_codes = {parent for _, _, _, parent in DIGITAL_ASSETS_REGULATORY_NODES if parent is not None}

    rows = [
        (
            "domain_digital_assets_regulatory",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in DIGITAL_ASSETS_REGULATORY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(DIGITAL_ASSETS_REGULATORY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_digital_assets_regulatory'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_digital_assets_regulatory'",
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
            [("naics_2022", code, "domain_digital_assets_regulatory", "primary") for code in naics_codes],
        )

    return count
