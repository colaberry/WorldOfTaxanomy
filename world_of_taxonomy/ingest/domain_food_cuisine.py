"""Food Service Cuisine and Menu Category Types domain taxonomy ingester.

Classifies food service operations by their cuisine type and menu format.
Orthogonal to venue/format type, revenue mix, and ownership model.
Based on Datassential and Technomic menu type taxonomy.
Used by food service investors, brand developers, location analytics firms,
and grocery retailers entering prepared food categories.

Code prefix: dfdcui_
System ID: domain_food_cuisine
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FOOD_CUISINE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dfdcui_american", "American and Comfort Food", 1, None),
    ("dfdcui_american_casual", "American casual dining (burgers, sandwiches, salads, all-day menu)", 2, "dfdcui_american"),
    ("dfdcui_american_bbq", "Barbecue and Southern comfort food (BBQ, fried chicken, soul food)", 2, "dfdcui_american"),
    ("dfdcui_asian", "Asian Cuisines", 1, None),
    ("dfdcui_asian_chinese", "Chinese: Cantonese, Sichuan, dim sum, American-Chinese", 2, "dfdcui_asian"),
    ("dfdcui_asian_japanese", "Japanese: sushi, ramen, izakaya, omakase, udon", 2, "dfdcui_asian"),
    ("dfdcui_asian_korean", "Korean: BBQ, bibimbap, fried chicken, Korean-fusion", 2, "dfdcui_asian"),
    ("dfdcui_asian_southeast", "Southeast Asian: Thai, Vietnamese, Filipino, Indonesian", 2, "dfdcui_asian"),
    ("dfdcui_european", "European Cuisines", 1, None),
    ("dfdcui_european_italian", "Italian: pizza, pasta, trattoria, fine dining Italian", 2, "dfdcui_european"),
    ("dfdcui_european_french", "French: brasserie, fine dining French, bistro, crepes", 2, "dfdcui_european"),
    ("dfdcui_european_mediterranean", "Mediterranean: Greek, Spanish, Middle Eastern-influenced", 2, "dfdcui_european"),
    ("dfdcui_latin", "Latin American Cuisines", 1, None),
    ("dfdcui_latin_mexican", "Mexican and Tex-Mex: tacos, burritos, fast-casual Mexican", 2, "dfdcui_latin"),
    ("dfdcui_latin_peruvian", "Peruvian and South American (ceviche, Nikkei, modern Peruvian)", 2, "dfdcui_latin"),
    ("dfdcui_health", "Health-Focused and Dietary Menu Formats", 1, None),
    ("dfdcui_health_vegan", "Vegan and plant-based restaurants", 2, "dfdcui_health"),
    ("dfdcui_health_clean", "Clean-label, organic, allergen-free dietary formats", 2, "dfdcui_health"),
    ("dfdcui_qsr", "Quick Service and Fast Food Formats", 1, None),
    ("dfdcui_qsr_burger", "QSR burger and sandwich chains", 2, "dfdcui_qsr"),
    ("dfdcui_qsr_pizza", "QSR and fast-casual pizza", 2, "dfdcui_qsr"),
    ("dfdcui_qsr_coffee", "Coffee shops and beverage-forward cafes (Starbucks, Dunkin)", 2, "dfdcui_qsr"),
]

_DOMAIN_ROW = (
    "domain_food_cuisine",
    "Food Service Cuisine and Menu Category Types",
    "Restaurant and food service cuisine type and menu format classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['7225', '7224', '4451']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_food_cuisine(conn) -> int:
    """Ingest Food Service Cuisine and Menu Category Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_food_cuisine'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_food_cuisine",
        "Food Service Cuisine and Menu Category Types",
        "Restaurant and food service cuisine type and menu format classification",
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

    parent_codes = {parent for _, _, _, parent in FOOD_CUISINE_NODES if parent is not None}

    rows = [
        (
            "domain_food_cuisine",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in FOOD_CUISINE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FOOD_CUISINE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_food_cuisine'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_food_cuisine'",
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
            [("naics_2022", code, "domain_food_cuisine", "primary") for code in naics_codes],
        )

    return count
