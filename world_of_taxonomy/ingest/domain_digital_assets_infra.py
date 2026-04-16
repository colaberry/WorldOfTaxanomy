"""Digital Assets Infrastructure Layer Types domain taxonomy ingester.

Classifies the infrastructure layers underpinning digital asset markets.
Orthogonal to asset type and regulatory framework.
Used by institutional investors evaluating provider risks, technology vendors
positioning products, and regulators assessing systemic risks in crypto plumbing.

Code prefix: ddainf_
System ID: domain_digital_assets_infra
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
DIGITAL_ASSETS_INFRA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ddainf_custody", "Custody and Key Management Infrastructure", 1, None),
    ("ddainf_custody_inst", "Institutional custody (Coinbase Custody, BitGo, Anchorage, Fireblocks)", 2, "ddainf_custody"),
    ("ddainf_custody_selfcust", "Self-custody wallets (hardware wallets - Ledger, Trezor; smart contract wallets)", 2, "ddainf_custody"),
    ("ddainf_custody_mpc", "MPC-based custody (multi-party computation key sharding)", 2, "ddainf_custody"),
    ("ddainf_exchange", "Exchange and Liquidity Infrastructure", 1, None),
    ("ddainf_exchange_cex", "Centralized exchanges (CEX: Coinbase, Binance, Kraken order books)", 2, "ddainf_exchange"),
    ("ddainf_exchange_dex", "Decentralized exchanges (DEX: Uniswap, Curve, dYdX AMM/CLOB)", 2, "ddainf_exchange"),
    ("ddainf_exchange_otc", "OTC desks and RFQ protocols (institutional block trading)", 2, "ddainf_exchange"),
    ("ddainf_exchange_prime", "Prime brokerage (lending, financing, clearing for institutions)", 2, "ddainf_exchange"),
    ("ddainf_settlement", "Settlement and Clearing Infrastructure", 1, None),
    ("ddainf_settlement_l1", "Layer-1 blockchain settlement (Bitcoin, Ethereum, Solana finality)", 2, "ddainf_settlement"),
    ("ddainf_settlement_l2", "Layer-2 scaling solutions (rollups, state channels, sidechains)", 2, "ddainf_settlement"),
    ("ddainf_settlement_bridge", "Cross-chain bridges and messaging protocols", 2, "ddainf_settlement"),
    ("ddainf_data", "On-Chain Data and Analytics Infrastructure", 1, None),
    ("ddainf_data_index", "Blockchain indexing and query (The Graph, Dune, Goldsky)", 2, "ddainf_data"),
    ("ddainf_data_oracle", "Oracle networks (Chainlink, Pyth, API3 price feeds)", 2, "ddainf_data"),
    ("ddainf_data_compliance", "Blockchain analytics and compliance (Chainalysis, TRM, Elliptic)", 2, "ddainf_data"),
]

_DOMAIN_ROW = (
    "domain_digital_assets_infra",
    "Digital Assets Infrastructure Layer Types",
    "Crypto and digital asset infrastructure layer classification: custody, exchange rails, settlement, data and analytics",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['5221', '5415', '5182']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_digital_assets_infra(conn) -> int:
    """Ingest Digital Assets Infrastructure Layer Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_digital_assets_infra'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_digital_assets_infra",
        "Digital Assets Infrastructure Layer Types",
        "Crypto and digital asset infrastructure layer classification: custody, exchange rails, settlement, data and analytics",
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

    parent_codes = {parent for _, _, _, parent in DIGITAL_ASSETS_INFRA_NODES if parent is not None}

    rows = [
        (
            "domain_digital_assets_infra",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in DIGITAL_ASSETS_INFRA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(DIGITAL_ASSETS_INFRA_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_digital_assets_infra'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_digital_assets_infra'",
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
            [("naics_2022", code, "domain_digital_assets_infra", "primary") for code in naics_codes],
        )

    return count
