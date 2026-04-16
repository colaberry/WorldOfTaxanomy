"""Wine and Spirits domain taxonomy ingester.

Organizes wine and spirits industry types into categories aligned with
NAICS 3121 (Beverage Manufacturing).

Code prefix: dws_
Categories: viticulture, wine production, distilled spirits,
craft brewing, beverage distribution.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
WINE_SPIRITS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Viticulture --
    ("dws_viti",               "Viticulture",                                       1, None),
    ("dws_viti_vineyard",      "Vineyard Management and Grape Growing",              2, "dws_viti"),
    ("dws_viti_nursery",       "Grapevine Nursery and Rootstock Propagation",        2, "dws_viti"),
    ("dws_viti_organic",       "Organic and Biodynamic Viticulture",                 2, "dws_viti"),
    ("dws_viti_harvest",       "Grape Harvesting and Sorting",                       2, "dws_viti"),

    # -- Wine Production --
    ("dws_wine",               "Wine Production",                                   1, None),
    ("dws_wine_still",         "Still Wine (red, white, rose)",                      2, "dws_wine"),
    ("dws_wine_sparkling",     "Sparkling Wine and Champagne",                       2, "dws_wine"),
    ("dws_wine_fortified",     "Fortified Wine (port, sherry, madeira)",             2, "dws_wine"),
    ("dws_wine_natural",       "Natural and Low-Intervention Wine",                  2, "dws_wine"),

    # -- Distilled Spirits --
    ("dws_spirit",             "Distilled Spirits",                                 1, None),
    ("dws_spirit_whiskey",     "Whiskey and Bourbon Distilling",                     2, "dws_spirit"),
    ("dws_spirit_vodka",       "Vodka and Neutral Spirits",                          2, "dws_spirit"),
    ("dws_spirit_rum",         "Rum and Cane Spirits",                               2, "dws_spirit"),
    ("dws_spirit_gin",         "Gin and Botanical Spirits",                          2, "dws_spirit"),
    ("dws_spirit_tequila",     "Tequila, Mezcal and Agave Spirits",                 2, "dws_spirit"),
    ("dws_spirit_brandy",      "Brandy and Cognac",                                  2, "dws_spirit"),

    # -- Craft Brewing --
    ("dws_brew",               "Craft Brewing",                                     1, None),
    ("dws_brew_ale",           "Ale and Lager Craft Brewing",                        2, "dws_brew"),
    ("dws_brew_cider",         "Hard Cider and Perry Production",                    2, "dws_brew"),
    ("dws_brew_seltzer",       "Hard Seltzer and Ready-to-Drink Beverages",          2, "dws_brew"),

    # -- Beverage Distribution --
    ("dws_dist",               "Beverage Distribution",                             1, None),
    ("dws_dist_wholesale",     "Wholesale Beverage Distribution",                    2, "dws_dist"),
    ("dws_dist_import",        "Import and Export of Wines and Spirits",             2, "dws_dist"),
    ("dws_dist_dtc",           "Direct-to-Consumer and Wine Club Sales",             2, "dws_dist"),
    ("dws_dist_ecomm",         "E-Commerce Beverage Platforms",                      2, "dws_dist"),
]

_DOMAIN_ROW = (
    "domain_wine_spirits",
    "Wine and Spirits Types",
    "Viticulture, wine production, distilled spirits, craft brewing, "
    "and beverage distribution domain taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link: 3121 (Beverage Manufacturing)
_NAICS_PREFIXES = ["3121"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_wine_spirits(conn) -> int:
    """Ingest Wine and Spirits domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_wine_spirits'), and links NAICS 3121 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_wine_spirits",
        "Wine and Spirits Types",
        "Viticulture, wine production, distilled spirits, craft brewing, "
        "and beverage distribution domain taxonomy",
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

    parent_codes = {parent for _, _, _, parent in WINE_SPIRITS_NODES if parent is not None}

    rows = [
        (
            "domain_wine_spirits",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in WINE_SPIRITS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(WINE_SPIRITS_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_wine_spirits'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_wine_spirits'",
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
            [("naics_2022", code, "domain_wine_spirits", "primary") for code in naics_codes],
        )

    return count
