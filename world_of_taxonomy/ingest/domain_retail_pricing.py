"""Retail Pricing Strategy Types domain taxonomy ingester.

Classifies retail businesses by their primary pricing strategy.
Orthogonal to channel type, merchandise category, and fulfillment model.
Used by retail strategists, category managers, pricing software vendors,
and equity analysts assessing gross margin and competitive positioning.

Code prefix: drtlprc_
System ID: domain_retail_pricing
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
RETAIL_PRICING_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("drtlprc_edlp", "Everyday Low Price (EDLP) Strategy", 1, None),
    ("drtlprc_edlp_pure", "Pure EDLP: stable low prices, minimal promotions (Walmart, Aldi)", 2, "drtlprc_edlp"),
    ("drtlprc_edlp_private", "EDLP with strong private-label focus for margin support", 2, "drtlprc_edlp"),
    ("drtlprc_hilo", "High-Low Promotional Pricing Strategy", 1, None),
    ("drtlprc_hilo_weekly", "Weekly circular / weekly ad-driven promotions", 2, "drtlprc_hilo"),
    ("drtlprc_hilo_loyalty", "Loyalty-card pricing (member vs. non-member pricing)", 2, "drtlprc_hilo"),
    ("drtlprc_hilo_event", "Event-based pricing (Black Friday, seasonal clearance)", 2, "drtlprc_hilo"),
    ("drtlprc_premium", "Premium and Luxury Pricing Strategy", 1, None),
    ("drtlprc_premium_prestige", "Prestige pricing (luxury brands, Veblen goods)", 2, "drtlprc_premium"),
    ("drtlprc_premium_quality", "Quality signaling pricing (organic, clean-label premiums)", 2, "drtlprc_premium"),
    ("drtlprc_dynamic", "Dynamic and Algorithmic Pricing Strategy", 1, None),
    ("drtlprc_dynamic_demand", "Demand-based dynamic repricing (surge, markdown optimization)", 2, "drtlprc_dynamic"),
    ("drtlprc_dynamic_competitive", "Competitive price matching and algorithmic parity", 2, "drtlprc_dynamic"),
    ("drtlprc_dynamic_personalized", "Personalized pricing using purchase history and propensity", 2, "drtlprc_dynamic"),
    ("drtlprc_subscription", "Subscription and Membership Pricing Models", 1, None),
    ("drtlprc_subscription_membership", "Membership fee + low retail prices (Costco, Sam's Club)", 2, "drtlprc_subscription"),
    ("drtlprc_subscription_auto", "Auto-replenishment subscriptions (Amazon Subscribe & Save)", 2, "drtlprc_subscription"),
    ("drtlprc_valueadd", "Value-Add and Bundle Pricing", 1, None),
    ("drtlprc_valueadd_bundle", "Product bundling for higher ticket and margin", 2, "drtlprc_valueadd"),
    ("drtlprc_valueadd_tiered", "Tiered good/better/best assortment pricing", 2, "drtlprc_valueadd"),
]

_DOMAIN_ROW = (
    "domain_retail_pricing",
    "Retail Pricing Strategy Types",
    "Retail pricing strategy and model classification: EDLP, hi-lo, premium, dynamic and subscription",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['44', '45', '4541']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_retail_pricing(conn) -> int:
    """Ingest Retail Pricing Strategy Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_retail_pricing'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_retail_pricing",
        "Retail Pricing Strategy Types",
        "Retail pricing strategy and model classification: EDLP, hi-lo, premium, dynamic and subscription",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in RETAIL_PRICING_NODES if parent is not None}

    rows = [
        (
            "domain_retail_pricing",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in RETAIL_PRICING_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(RETAIL_PRICING_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_retail_pricing'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_retail_pricing'",
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
            [("naics_2022", code, "domain_retail_pricing", "primary") for code in naics_codes],
        )

    return count
