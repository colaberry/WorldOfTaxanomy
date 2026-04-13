"""Retail Channel Type domain taxonomy ingester.

Retail channel taxonomy organizes sales channels and store formats:
  Channel Type  (drc_channel*) - brick-and-mortar, e-commerce, omnichannel, direct-to-consumer
  Store Format  (drc_format*)  - department, specialty, discount, convenience, warehouse club
  Customer Seg  (drc_cust*)    - mass market, premium, value, B2B, direct

Source: NRF (National Retail Federation) + Census retail trade classifications. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
RETAIL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Channel Type category --
    ("drc_channel",       "Retail Channel Type",                              1, None),
    ("drc_channel_bam",   "Brick-and-Mortar (physical stores)",              2, "drc_channel"),
    ("drc_channel_ecom",  "E-Commerce (online-only, marketplace)",           2, "drc_channel"),
    ("drc_channel_omni",  "Omnichannel (buy-online-pickup-in-store, BOPIS)", 2, "drc_channel"),
    ("drc_channel_dtc",   "Direct-to-Consumer (DTC, brand.com)",             2, "drc_channel"),
    ("drc_channel_social","Social Commerce (Instagram, TikTok Shop)",        2, "drc_channel"),

    # -- Store Format category --
    ("drc_format",           "Store Format",                                  1, None),
    ("drc_format_dept",      "Department Store (full-line, anchor)",         2, "drc_format"),
    ("drc_format_specialty", "Specialty Store (single-category focus)",      2, "drc_format"),
    ("drc_format_discount",  "Discount and Off-Price (TJ Maxx, Ross)",       2, "drc_format"),
    ("drc_format_conv",      "Convenience Store (c-store, small format)",    2, "drc_format"),
    ("drc_format_warehouse", "Warehouse Club (Costco, Sam's Club)",          2, "drc_format"),
    ("drc_format_grocery",   "Grocery and Supermarket",                       2, "drc_format"),
    ("drc_format_pop",       "Pop-Up and Temporary Retail",                  2, "drc_format"),

    # -- Customer Segment category --
    ("drc_cust",         "Customer Segment",                                  1, None),
    ("drc_cust_mass",    "Mass Market Consumer",                              2, "drc_cust"),
    ("drc_cust_premium", "Premium and Luxury Consumer",                      2, "drc_cust"),
    ("drc_cust_value",   "Value-Seeking Consumer",                            2, "drc_cust"),
    ("drc_cust_b2b",     "Business-to-Business (B2B retail)",                2, "drc_cust"),
]

_DOMAIN_ROW = (
    "domain_retail_channel",
    "Retail Channel Types",
    "Channel type, store format and customer segment taxonomy for retail trade",
    "WorldOfTaxanomy",
    None,
)

_NAICS_PREFIXES = ["44", "45"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific channel types."""
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


async def ingest_domain_retail_channel(conn) -> int:
    """Ingest Retail Channel Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_retail_channel'), and links NAICS 44-45xxx nodes
    via node_taxonomy_link.

    Returns total channel node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_retail_channel",
        "Retail Channel Types",
        "Channel type, store format and customer segment taxonomy for retail trade",
        "1.0",
        "United States",
        "WorldOfTaxanomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in RETAIL_NODES if parent is not None}

    rows = [
        (
            "domain_retail_channel",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in RETAIL_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(RETAIL_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_retail_channel'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_retail_channel'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' "
            "AND (code LIKE '44%' OR code LIKE '45%')"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_retail_channel", "primary") for code in naics_codes],
    )

    return count
