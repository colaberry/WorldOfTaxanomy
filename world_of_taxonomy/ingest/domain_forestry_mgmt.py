"""Forestry Management domain taxonomy ingester.

Organizes forestry management sector types aligned with NAICS 1131
(Timber tract operations), NAICS 1132 (Forest nurseries and gathering
of forest products), and NAICS 1133 (Logging) covering timber production,
reforestation, forest conservation, agroforestry, and forest products.

Code prefix: dfm_
Categories: Timber Production, Reforestation, Forest Conservation,
Agroforestry, Forest Products.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Timber Production --
    ("dfm_timber",             "Timber Production",                                  1, None),
    ("dfm_timber_softwood",    "Softwood Timber Harvesting (pine, spruce, fir)",    2, "dfm_timber"),
    ("dfm_timber_hardwood",    "Hardwood Timber Harvesting (oak, maple, teak)",     2, "dfm_timber"),
    ("dfm_timber_plantation",  "Plantation Forestry and Tree Farming",              2, "dfm_timber"),
    ("dfm_timber_selective",   "Selective and Sustainable Logging",                  2, "dfm_timber"),

    # -- Reforestation --
    ("dfm_reforest",           "Reforestation",                                      1, None),
    ("dfm_reforest_nursery",   "Forest Nursery and Seedling Production",            2, "dfm_reforest"),
    ("dfm_reforest_plant",     "Reforestation and Afforestation Programs",          2, "dfm_reforest"),
    ("dfm_reforest_restore",   "Degraded Land Restoration and Rewilding",           2, "dfm_reforest"),
    ("dfm_reforest_carbon",    "Carbon Offset and Forest Credit Programs",          2, "dfm_reforest"),

    # -- Forest Conservation --
    ("dfm_conserve",           "Forest Conservation",                                1, None),
    ("dfm_conserve_protect",   "Protected Area and National Forest Management",     2, "dfm_conserve"),
    ("dfm_conserve_fire",      "Wildfire Prevention and Forest Fire Management",    2, "dfm_conserve"),
    ("dfm_conserve_pest",      "Forest Pest and Disease Control",                    2, "dfm_conserve"),
    ("dfm_conserve_biodiv",    "Biodiversity Monitoring and Wildlife Habitat",       2, "dfm_conserve"),

    # -- Agroforestry --
    ("dfm_agro",               "Agroforestry",                                       1, None),
    ("dfm_agro_silvo",         "Silvopasture and Livestock-Forest Integration",      2, "dfm_agro"),
    ("dfm_agro_alley",         "Alley Cropping and Intercropping Systems",           2, "dfm_agro"),
    ("dfm_agro_riparian",      "Riparian and Windbreak Buffer Plantings",            2, "dfm_agro"),
    ("dfm_agro_tropical",      "Tropical Agroforestry and Shade-Grown Crops",        2, "dfm_agro"),

    # -- Forest Products --
    ("dfm_product",            "Forest Products",                                    1, None),
    ("dfm_product_lumber",     "Lumber and Sawn Wood Products",                      2, "dfm_product"),
    ("dfm_product_pulp",       "Pulp, Paper, and Cellulose Products",                2, "dfm_product"),
    ("dfm_product_nontimber",  "Non-Timber Forest Products (resins, nuts, fungi)",  2, "dfm_product"),
    ("dfm_product_biomass",    "Forest Biomass and Wood Energy Products",            2, "dfm_product"),
]

_DOMAIN_ROW = (
    "domain_forestry_mgmt",
    "Forestry Management Types",
    "Forestry management sector types covering timber production, "
    "reforestation, forest conservation, agroforestry, and forest products",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 1131 (Timber tracts), 1132 (Forest nurseries), 1133 (Logging)
_NAICS_PREFIXES = ["1131", "1132", "1133"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific forestry management types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_forestry_mgmt(conn) -> int:
    """Ingest Forestry Management domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_forestry_mgmt'), and links NAICS 1131/1132/1133
    nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_forestry_mgmt",
        "Forestry Management Types",
        "Forestry management sector types covering timber production, "
        "reforestation, forest conservation, agroforestry, and forest products",
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

    parent_codes = {parent for _, _, _, parent in NODES if parent is not None}

    rows = [
        (
            "domain_forestry_mgmt",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_forestry_mgmt'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_forestry_mgmt'",
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
            [("naics_2022", code, "domain_forestry_mgmt", "primary") for code in naics_codes],
        )

    return count
