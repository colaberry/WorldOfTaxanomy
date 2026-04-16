"""Food Service and Accommodation domain taxonomy ingester.

Food service and accommodation taxonomy organizes hospitality categories (NAICS 72):
  Lodging Type   (dfs_lodge*)   - full-service hotel, limited-service, extended-stay, motel
  Cuisine Type   (dfs_cuisine*) - American, Italian, Asian, Mexican, Mediterranean, other
  Service Model  (dfs_svc*)     - fine dining, casual, fast casual, QSR, food truck

Source: STR (Smith Travel Research) + NRA (National Restaurant Association).
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FOOD_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Lodging Type category --
    ("dfs_lodge",         "Lodging Type",                                     1, None),
    ("dfs_lodge_full",    "Full-Service Hotel (luxury, upscale, branded)",   2, "dfs_lodge"),
    ("dfs_lodge_select",  "Select-Service and Limited-Service Hotel",        2, "dfs_lodge"),
    ("dfs_lodge_ext",     "Extended-Stay Hotel (residential-style suites)",  2, "dfs_lodge"),
    ("dfs_lodge_motel",   "Motel and Economy Lodging",                       2, "dfs_lodge"),
    ("dfs_lodge_resort",  "Resort and Conference Center",                     2, "dfs_lodge"),
    ("dfs_lodge_bnb",     "Bed and Breakfast / Boutique Inn",                2, "dfs_lodge"),
    ("dfs_lodge_str",     "Short-Term Rental (Airbnb, VRBO, vacation home)", 2, "dfs_lodge"),

    # -- Cuisine Type category --
    ("dfs_cuisine",          "Cuisine Type",                                  1, None),
    ("dfs_cuisine_american", "American (burgers, barbecue, diner, comfort)", 2, "dfs_cuisine"),
    ("dfs_cuisine_italian",  "Italian (pizza, pasta, trattoria)",            2, "dfs_cuisine"),
    ("dfs_cuisine_asian",    "Asian (Chinese, Japanese, Korean, Thai, Indian)", 2, "dfs_cuisine"),
    ("dfs_cuisine_mexican",  "Mexican and Latin American",                   2, "dfs_cuisine"),
    ("dfs_cuisine_med",      "Mediterranean and Middle Eastern",             2, "dfs_cuisine"),
    ("dfs_cuisine_other",    "Other International and Fusion Cuisine",       2, "dfs_cuisine"),

    # -- Service Model category --
    ("dfs_svc",        "Food Service Model",                                  1, None),
    ("dfs_svc_fine",   "Fine Dining (white tablecloth, prix fixe)",          2, "dfs_svc"),
    ("dfs_svc_casual", "Casual Dining (full-service, family restaurant)",    2, "dfs_svc"),
    ("dfs_svc_fc",     "Fast Casual (counter service, better ingredients)",  2, "dfs_svc"),
    ("dfs_svc_qsr",    "Quick Service Restaurant (QSR, fast food)",         2, "dfs_svc"),
    ("dfs_svc_bar",    "Bar, Tavern, and Brewery (food-focused)",            2, "dfs_svc"),
    ("dfs_svc_food",   "Food Truck and Pop-Up",                              2, "dfs_svc"),
    ("dfs_svc_catering","Catering and Event Food Service",                   2, "dfs_svc"),
]

_DOMAIN_ROW = (
    "domain_food_service",
    "Food Service and Accommodation",
    "Lodging type, cuisine type and food service model taxonomy for NAICS 72 hospitality sector",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["72"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific food/lodging types."""
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


async def ingest_domain_food_service(conn) -> int:
    """Ingest Food Service and Accommodation domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_food_service'), and links NAICS 72xxx nodes
    via node_taxonomy_link.

    Returns total food/lodging node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_food_service",
        "Food Service and Accommodation",
        "Lodging type, cuisine type and food service model taxonomy for NAICS 72 hospitality sector",
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

    parent_codes = {parent for _, _, _, parent in FOOD_NODES if parent is not None}

    rows = [
        (
            "domain_food_service",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in FOOD_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FOOD_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_food_service'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_food_service'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '72%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_food_service", "primary") for code in naics_codes],
    )

    return count
