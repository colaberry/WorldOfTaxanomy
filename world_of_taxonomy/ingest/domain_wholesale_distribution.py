"""Wholesale Distribution Strategy and Model Types domain taxonomy ingester.

Classifies wholesale distribution operations by their distribution strategy.
Orthogonal to product category and regulatory category.
Based on Kotler channel design framework and NAICS wholesale trade categories.
Used by manufacturer channel managers, distributor network designers,
and strategy consultants optimizing channel mix.

Code prefix: dwsldist_
System ID: domain_wholesale_distribution
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
WHOLESALE_DISTRIBUTION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dwsldist_direct", "Direct Distribution Strategies", 1, None),
    ("dwsldist_direct_manufacturer", "Direct: manufacturer sells directly to end retailer or user", 2, "dwsldist_direct"),
    ("dwsldist_direct_ecomm", "Direct e-commerce B2B (manufacturer direct via digital platform)", 2, "dwsldist_direct"),
    ("dwsldist_exclusive", "Exclusive Distribution Strategies", 1, None),
    ("dwsldist_exclusive_single", "Single exclusive distributor per territory (luxury goods, specialty)", 2, "dwsldist_exclusive"),
    ("dwsldist_exclusive_authorized", "Authorized dealer network (auto, industrial equipment)", 2, "dwsldist_exclusive"),
    ("dwsldist_selective", "Selective Distribution Strategies", 1, None),
    ("dwsldist_selective_limited", "Limited distribution to qualified channel partners", 2, "dwsldist_selective"),
    ("dwsldist_selective_specialty", "Specialty channel focus (hardware stores only, pharma only)", 2, "dwsldist_selective"),
    ("dwsldist_intensive", "Intensive and Mass Distribution Strategies", 1, None),
    ("dwsldist_intensive_mass", "Mass distribution: maximum availability through all channel types", 2, "dwsldist_intensive"),
    ("dwsldist_intensive_broadline", "Broadline distributor carries full category for all customer types", 2, "dwsldist_intensive"),
    ("dwsldist_dropship", "Drop-Ship and Fulfillment Models", 1, None),
    ("dwsldist_dropship_traditional", "Traditional drop-ship: distributor ships from own stock on retailer order", 2, "dwsldist_dropship"),
    ("dwsldist_dropship_marketplace", "Marketplace fulfillment (3P seller on Amazon/Walmart marketplace)", 2, "dwsldist_dropship"),
    ("dwsldist_value", "Value-Added Distribution Models", 1, None),
    ("dwsldist_value_kitting", "Kitting and light assembly at distribution center", 2, "dwsldist_value"),
    ("dwsldist_value_private", "Private-label and co-branding distribution", 2, "dwsldist_value"),
    ("dwsldist_value_vmi", "Vendor-managed inventory (distributor manages end-customer stock)", 2, "dwsldist_value"),
]

_DOMAIN_ROW = (
    "domain_wholesale_distribution",
    "Wholesale Distribution Strategy and Model Types",
    "Wholesale and distribution strategy model classification: direct, exclusive, selective, intensive, drop-ship",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['42', '4231', '4244', '4245']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_wholesale_distribution(conn) -> int:
    """Ingest Wholesale Distribution Strategy and Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_wholesale_distribution'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_wholesale_distribution",
        "Wholesale Distribution Strategy and Model Types",
        "Wholesale and distribution strategy model classification: direct, exclusive, selective, intensive, drop-ship",
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

    parent_codes = {parent for _, _, _, parent in WHOLESALE_DISTRIBUTION_NODES if parent is not None}

    rows = [
        (
            "domain_wholesale_distribution",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in WHOLESALE_DISTRIBUTION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(WHOLESALE_DISTRIBUTION_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_wholesale_distribution'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_wholesale_distribution'",
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
            [("naics_2022", code, "domain_wholesale_distribution", "primary") for code in naics_codes],
        )

    return count
