"""Textile and Fashion domain taxonomy ingester.

Organizes textile and fashion sector types aligned with NAICS 3131
(Fiber, yarn, and thread mills), NAICS 3132 (Fabric mills), and
NAICS 3133 (Textile and fabric finishing) covering fiber and yarn,
fabric manufacturing, apparel design, fast fashion, and luxury/haute couture.

Code prefix: dtf_
Categories: Fiber and Yarn, Fabric Manufacturing, Apparel Design,
Fast Fashion, Luxury and Haute Couture.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Fiber and Yarn --
    ("dtf_fiber",             "Fiber and Yarn",                                      1, None),
    ("dtf_fiber_natural",     "Natural Fiber Processing (cotton, wool, silk, linen)", 2, "dtf_fiber"),
    ("dtf_fiber_synthetic",   "Synthetic Fiber Production (polyester, nylon)",        2, "dtf_fiber"),
    ("dtf_fiber_blend",       "Blended and Composite Yarn Manufacturing",            2, "dtf_fiber"),
    ("dtf_fiber_reclaim",     "Recycled and Reclaimed Fiber Processing",             2, "dtf_fiber"),

    # -- Fabric Manufacturing --
    ("dtf_fabric",            "Fabric Manufacturing",                                1, None),
    ("dtf_fabric_woven",      "Woven Fabric Production",                             2, "dtf_fabric"),
    ("dtf_fabric_knit",       "Knitted and Crocheted Fabric Production",             2, "dtf_fabric"),
    ("dtf_fabric_nonwoven",   "Nonwoven Fabric and Technical Textiles",              2, "dtf_fabric"),
    ("dtf_fabric_dye",        "Textile Dyeing and Finishing",                        2, "dtf_fabric"),

    # -- Apparel Design --
    ("dtf_apparel",           "Apparel Design",                                      1, None),
    ("dtf_apparel_rtw",       "Ready-to-Wear Apparel Design",                        2, "dtf_apparel"),
    ("dtf_apparel_sport",     "Sportswear and Activewear Design",                    2, "dtf_apparel"),
    ("dtf_apparel_sustain",   "Sustainable and Ethical Fashion Design",              2, "dtf_apparel"),
    ("dtf_apparel_access",    "Fashion Accessories and Footwear Design",             2, "dtf_apparel"),

    # -- Fast Fashion --
    ("dtf_fast",              "Fast Fashion",                                        1, None),
    ("dtf_fast_mass",         "Mass-Market Fast Fashion Production",                 2, "dtf_fast"),
    ("dtf_fast_ultra",        "Ultra-Fast Fashion and On-Demand Manufacturing",      2, "dtf_fast"),
    ("dtf_fast_ecomm",        "Direct-to-Consumer Fashion E-Commerce",               2, "dtf_fast"),
    ("dtf_fast_supply",       "Fast Fashion Supply Chain and Sourcing",              2, "dtf_fast"),

    # -- Luxury and Haute Couture --
    ("dtf_luxury",            "Luxury and Haute Couture",                            1, None),
    ("dtf_luxury_couture",    "Haute Couture and Bespoke Fashion",                   2, "dtf_luxury"),
    ("dtf_luxury_brand",      "Luxury Brand Management and Licensing",               2, "dtf_luxury"),
    ("dtf_luxury_artisan",    "Artisan Craftsmanship and Heritage Textiles",         2, "dtf_luxury"),
    ("dtf_luxury_resale",     "Luxury Resale and Authentication Services",           2, "dtf_luxury"),
]

_DOMAIN_ROW = (
    "domain_textile_fashion",
    "Textile and Fashion Types",
    "Textile and fashion sector types covering fiber and yarn, fabric "
    "manufacturing, apparel design, fast fashion, and luxury/haute couture",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 3131 (Fiber/yarn), 3132 (Fabric mills), 3133 (Finishing)
_NAICS_PREFIXES = ["3131", "3132", "3133"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific textile/fashion types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_textile_fashion(conn) -> int:
    """Ingest Textile and Fashion domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_textile_fashion'), and links NAICS 3131/3132/3133
    nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_textile_fashion",
        "Textile and Fashion Types",
        "Textile and fashion sector types covering fiber and yarn, fabric "
        "manufacturing, apparel design, fast fashion, and luxury/haute couture",
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
            "domain_textile_fashion",
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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_textile_fashion'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_textile_fashion'",
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
            [("naics_2022", code, "domain_textile_fashion", "primary") for code in naics_codes],
        )

    return count
