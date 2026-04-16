"""Agriculture Crop Type Domain Taxonomy ingester.

Crop taxonomy organizes agricultural crop types into categories:
  Cereal Grains  (dac_grain*)  - wheat, corn, rice, barley, oats, sorghum
  Oilseeds       (dac_oil*)    - soybeans, canola, sunflower, peanuts, cottonseed
  Vegetables     (dac_veg*)    - root, leafy, fruiting, legume vegetables
  Fruits/Trees   (dac_fruit*)  - citrus, pome, stone, tropical, berries
  Fiber Crops    (dac_fiber*)  - cotton, hemp, flax
  Sugar Crops    (dac_sugar*)  - sugarcane, sugar beets
  Forages        (dac_forage*) - hay, alfalfa, silage, pasture grasses
  Specialty      (dac_spec*)   - tobacco, hops, herbs, spices

Source: FAO commodity classification + USDA crop categories. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CROP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Cereal Grains category --
    ("dac_grain",         "Cereal Grains",                                  1, None),
    ("dac_grain_wheat",   "Wheat (hard red, soft red, winter, spring)",     2, "dac_grain"),
    ("dac_grain_corn",    "Corn / Maize (field corn, sweet corn)",          2, "dac_grain"),
    ("dac_grain_rice",    "Rice (long grain, medium grain, short grain)",   2, "dac_grain"),
    ("dac_grain_sorghum", "Sorghum / Milo (grain sorghum)",                 2, "dac_grain"),
    ("dac_grain_barley",  "Barley (feed barley, malting barley)",           2, "dac_grain"),
    ("dac_grain_oats",    "Oats (feed oats, milling oats)",                 2, "dac_grain"),
    ("dac_grain_rye",     "Rye",                                            2, "dac_grain"),
    ("dac_grain_millet",  "Millet (proso, foxtail, pearl)",                 2, "dac_grain"),

    # -- Oilseeds category --
    ("dac_oil",           "Oilseeds",                                       1, None),
    ("dac_oil_soy",       "Soybeans",                                       2, "dac_oil"),
    ("dac_oil_canola",    "Canola / Rapeseed",                              2, "dac_oil"),
    ("dac_oil_sunflower", "Sunflower",                                      2, "dac_oil"),
    ("dac_oil_peanut",    "Peanuts / Groundnuts",                           2, "dac_oil"),
    ("dac_oil_cotton",    "Cottonseed (oil use)",                           2, "dac_oil"),
    ("dac_oil_flaxseed",  "Flaxseed / Linseed",                            2, "dac_oil"),

    # -- Vegetables category --
    ("dac_veg",           "Vegetables",                                     1, None),
    ("dac_veg_root",      "Root and Tuber Vegetables (potato, carrot, beet)", 2, "dac_veg"),
    ("dac_veg_leafy",     "Leafy and Stem Vegetables (lettuce, spinach, cabbage)", 2, "dac_veg"),
    ("dac_veg_fruiting",  "Fruiting Vegetables (tomato, pepper, cucumber, squash)", 2, "dac_veg"),
    ("dac_veg_legume",    "Legume Vegetables (green beans, peas, edamame)", 2, "dac_veg"),
    ("dac_veg_allium",    "Allium Vegetables (onion, garlic, leek)",        2, "dac_veg"),
    ("dac_veg_brassica",  "Brassica Vegetables (broccoli, cauliflower, kale)", 2, "dac_veg"),

    # -- Fruits and Tree Crops category --
    ("dac_fruit",         "Fruits and Tree Crops",                          1, None),
    ("dac_fruit_citrus",  "Citrus (orange, lemon, lime, grapefruit)",       2, "dac_fruit"),
    ("dac_fruit_pome",    "Pome Fruits (apple, pear, quince)",              2, "dac_fruit"),
    ("dac_fruit_stone",   "Stone Fruits (peach, plum, cherry, apricot)",    2, "dac_fruit"),
    ("dac_fruit_berry",   "Berries (strawberry, blueberry, grape)",         2, "dac_fruit"),
    ("dac_fruit_tropical","Tropical Fruits (banana, avocado, mango, papaya)", 2, "dac_fruit"),
    ("dac_fruit_nut",     "Tree Nuts (almond, walnut, pecan, pistachio)",   2, "dac_fruit"),

    # -- Fiber Crops category --
    ("dac_fiber",         "Fiber Crops",                                    1, None),
    ("dac_fiber_cotton",  "Cotton (upland cotton, pima cotton)",            2, "dac_fiber"),
    ("dac_fiber_hemp",    "Hemp (industrial hemp)",                         2, "dac_fiber"),
    ("dac_fiber_flax",    "Flax (fiber flax)",                              2, "dac_fiber"),

    # -- Sugar Crops category --
    ("dac_sugar",         "Sugar Crops",                                    1, None),
    ("dac_sugar_cane",    "Sugarcane",                                      2, "dac_sugar"),
    ("dac_sugar_beet",    "Sugar Beets",                                    2, "dac_sugar"),

    # -- Forages and Feed category --
    ("dac_forage",        "Forages and Feed Crops",                         1, None),
    ("dac_forage_alfalfa","Alfalfa / Hay (alfalfa, timothy, orchard grass)",2, "dac_forage"),
    ("dac_forage_silage", "Silage Crops (corn silage, sorghum silage)",     2, "dac_forage"),
    ("dac_forage_pasture","Pasture Grasses (bermuda, fescue, bluegrass)",   2, "dac_forage"),
    ("dac_forage_clover", "Clover and Legume Forages",                      2, "dac_forage"),

    # -- Specialty Crops category --
    ("dac_spec",          "Specialty and Industrial Crops",                 1, None),
    ("dac_spec_tobacco",  "Tobacco (flue-cured, burley, fire-cured)",       2, "dac_spec"),
    ("dac_spec_hops",     "Hops",                                           2, "dac_spec"),
    ("dac_spec_herbs",    "Herbs and Spices (mint, basil, cilantro)",       2, "dac_spec"),
    ("dac_spec_mushroom", "Mushrooms and Fungi",                            2, "dac_spec"),
    ("dac_spec_nursery",  "Nursery and Greenhouse Crops (flowers, trees)",  2, "dac_spec"),
]

_DOMAIN_ROW = (
    "domain_ag_crop",
    "Agricultural Crop Types",
    "Cereal grains, oilseeds, vegetables, fruits, fiber, sugar, forage and specialty crop taxonomy",
    "WorldOfTaxonomy",
    None,  # url
)

# NAICS prefixes to link (111xxx = Crop Production)
_NAICS_PREFIXES = ["111"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific crop types."""
    parts = code.split("_")
    # 'dac_grain'       -> ['dac', 'grain']         -> level 1
    # 'dac_grain_wheat' -> ['dac', 'grain', 'wheat'] -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_ag_crop(conn) -> int:
    """Ingest Agricultural Crop Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_ag_crop'), and links NAICS 111xxx nodes
    via node_taxonomy_link.

    Returns total crop node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_ag_crop",
        "Agricultural Crop Types",
        "Cereal grains, oilseeds, vegetables, fruits, fiber, sugar, forage and specialty crop taxonomy",
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

    parent_codes = {parent for _, _, _, parent in CROP_NODES if parent is not None}

    rows = [
        (
            "domain_ag_crop",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CROP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CROP_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_ag_crop'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_ag_crop'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '111%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_ag_crop", "primary") for code in naics_codes],
    )

    return count
