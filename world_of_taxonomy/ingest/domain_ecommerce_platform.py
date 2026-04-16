"""E-Commerce Platform domain taxonomy ingester.

E-commerce platform taxonomy organizes online commerce models and channels:
  B2C Marketplace  (dep_b2c*)    - general, vertical, flash sale, auction
  B2B Commerce     (dep_b2b*)    - wholesale, procurement, MRO, industrial
  D2C Platform     (dep_d2c*)    - brand-owned, subscription, custom, dropship
  Social Commerce  (dep_social*) - live stream, shoppable posts, group buying
  Cross-Border     (dep_cross*)  - global marketplace, localized, duty-free

Source: eMarketer and US Census e-commerce classifications.
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ECOMMERCE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- B2C Marketplaces category --
    ("dep_b2c",              "B2C Marketplaces",                                      1, None),
    ("dep_b2c_general",      "General Marketplace (Amazon, eBay, Walmart)",           2, "dep_b2c"),
    ("dep_b2c_vertical",     "Vertical Marketplace (Etsy, StockX, Zillow)",          2, "dep_b2c"),
    ("dep_b2c_flash",        "Flash Sale and Daily Deal (Groupon, Woot)",             2, "dep_b2c"),
    ("dep_b2c_auction",      "Online Auction and Bidding Platform",                   2, "dep_b2c"),
    ("dep_b2c_rental",       "Rental and Sharing Marketplace (Airbnb, Turo)",        2, "dep_b2c"),

    # -- B2B Commerce category --
    ("dep_b2b",              "B2B Commerce",                                           1, None),
    ("dep_b2b_wholesale",    "B2B Wholesale Marketplace (Alibaba, Faire)",            2, "dep_b2b"),
    ("dep_b2b_procure",      "E-Procurement Platform (Ariba, Coupa, Jaggaer)",       2, "dep_b2b"),
    ("dep_b2b_mro",          "MRO and Industrial Supply (Grainger, McMaster-Carr)",   2, "dep_b2b"),
    ("dep_b2b_saas",         "B2B SaaS Commerce (Shopify Plus, BigCommerce B2B)",    2, "dep_b2b"),

    # -- D2C Platforms category --
    ("dep_d2c",              "Direct-to-Consumer (D2C) Platforms",                     1, None),
    ("dep_d2c_brand",        "Brand-Owned Storefront (Shopify, WooCommerce)",         2, "dep_d2c"),
    ("dep_d2c_sub",          "Subscription Box and Recurring Commerce",               2, "dep_d2c"),
    ("dep_d2c_custom",       "Custom and Made-to-Order Platform (configurator)",      2, "dep_d2c"),
    ("dep_d2c_dropship",     "Dropship and Print-on-Demand Platform",                 2, "dep_d2c"),

    # -- Social Commerce category --
    ("dep_social",           "Social Commerce",                                        1, None),
    ("dep_social_live",      "Live Stream Shopping (TikTok Shop, Amazon Live)",       2, "dep_social"),
    ("dep_social_shoppable", "Shoppable Posts and Stories (Instagram, Pinterest)",    2, "dep_social"),
    ("dep_social_group",     "Group Buying and Community Commerce (Pinduoduo)",       2, "dep_social"),
    ("dep_social_creator",   "Creator and Affiliate Storefront (LTK, Stan Store)",   2, "dep_social"),

    # -- Cross-Border Commerce category --
    ("dep_cross",            "Cross-Border Commerce",                                  1, None),
    ("dep_cross_global",     "Global Marketplace (cross-border fulfillment, FBA)",    2, "dep_cross"),
    ("dep_cross_local",      "Localized Platform (Mercado Libre, Flipkart, Coupang)",2, "dep_cross"),
    ("dep_cross_duty",       "Duty-Free and Tax-Exempt Commerce (IOSS, Section 321)",2, "dep_cross"),
    ("dep_cross_logistics",  "Cross-Border Logistics Platform (freight, customs)",    2, "dep_cross"),
]

_DOMAIN_ROW = (
    "domain_ecommerce_platform",
    "E-Commerce Platform Types",
    "B2C marketplace, B2B commerce, D2C, social commerce and cross-border taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["4541"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific e-commerce types."""
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


async def ingest_domain_ecommerce_platform(conn) -> int:
    """Ingest E-Commerce Platform domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_ecommerce_platform'), and links NAICS 4541xxx nodes
    via node_taxonomy_link.

    Returns total e-commerce platform node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_ecommerce_platform",
        "E-Commerce Platform Types",
        "B2C marketplace, B2B commerce, D2C, social commerce and cross-border taxonomy",
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

    parent_codes = {parent for _, _, _, parent in ECOMMERCE_NODES if parent is not None}

    rows = [
        (
            "domain_ecommerce_platform",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ECOMMERCE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ECOMMERCE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_ecommerce_platform'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_ecommerce_platform'",
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
            [("naics_2022", code, "domain_ecommerce_platform", "primary") for code in naics_codes],
        )

    return count
