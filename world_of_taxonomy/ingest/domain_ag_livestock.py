"""Agriculture Livestock Category domain taxonomy ingester.

Livestock taxonomy organizes animal agriculture into categories:
  Cattle    (dal_cattle*)  - beef, dairy
  Swine     (dal_swine*)   - market hogs, breeding stock
  Poultry   (dal_poultry*) - broilers, layers, turkeys
  Sheep/Goat (dal_small*)  - sheep, goats
  Equine    (dal_equine*)  - horses, mules
  Aqua      (dal_aqua*)    - fish, shellfish (aquaculture)
  Other     (dal_other*)   - bees, rabbits, specialty animals

Source: USDA NASS livestock categories. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
LIVESTOCK_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Cattle category --
    ("dal_cattle",        "Cattle",                                          1, None),
    ("dal_cattle_beef",   "Beef Cattle (cow-calf, stocker, feedlot)",       2, "dal_cattle"),
    ("dal_cattle_dairy",  "Dairy Cattle (Holstein, Jersey, Brown Swiss)",   2, "dal_cattle"),

    # -- Swine category --
    ("dal_swine",         "Swine",                                           1, None),
    ("dal_swine_market",  "Market Hogs (feeder pigs, finishing)",            2, "dal_swine"),
    ("dal_swine_breed",   "Breeding Stock (sows, boars, gilts)",            2, "dal_swine"),

    # -- Poultry category --
    ("dal_poultry",           "Poultry",                                     1, None),
    ("dal_poultry_broiler",   "Broilers (meat chickens)",                   2, "dal_poultry"),
    ("dal_poultry_layer",     "Layers (egg-laying hens)",                   2, "dal_poultry"),
    ("dal_poultry_turkey",    "Turkeys",                                     2, "dal_poultry"),
    ("dal_poultry_duck",      "Ducks and Geese",                            2, "dal_poultry"),

    # -- Small ruminants category --
    ("dal_small",             "Sheep and Goats",                             1, None),
    ("dal_small_sheep",       "Sheep (wool, lamb, meat)",                   2, "dal_small"),
    ("dal_small_goat",        "Goats (dairy, meat, fiber)",                 2, "dal_small"),

    # -- Equine category --
    ("dal_equine",            "Equine",                                      1, None),
    ("dal_equine_horse",      "Horses (work, sport, pleasure)",             2, "dal_equine"),
    ("dal_equine_mule",       "Mules and Donkeys",                          2, "dal_equine"),

    # -- Aquaculture category --
    ("dal_aqua",              "Aquaculture",                                 1, None),
    ("dal_aqua_fish",         "Finfish (catfish, tilapia, salmon, trout)",  2, "dal_aqua"),
    ("dal_aqua_shell",        "Shellfish (oysters, shrimp, clams, crab)",   2, "dal_aqua"),
    ("dal_aqua_algae",        "Aquatic Plants and Algae",                   2, "dal_aqua"),

    # -- Other livestock category --
    ("dal_other",             "Other Livestock and Specialty Animals",       1, None),
    ("dal_other_bee",         "Honeybees and Apiculture",                   2, "dal_other"),
    ("dal_other_rabbit",      "Rabbits",                                     2, "dal_other"),
    ("dal_other_bison",       "Bison and Buffalo",                          2, "dal_other"),
    ("dal_other_deer",        "Deer and Elk (captive cervids)",             2, "dal_other"),
    ("dal_other_emu",         "Emus, Ostriches, and Ratites",               2, "dal_other"),
]

_DOMAIN_ROW = (
    "domain_ag_livestock",
    "Agricultural Livestock Categories",
    "Cattle, swine, poultry, sheep, equine, aquaculture and specialty livestock taxonomy",
    "WorldOfTaxonomy",
    None,  # url
)

# NAICS prefixes to link (112xxx = Animal Production and Aquaculture)
_NAICS_PREFIXES = ["112"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific livestock types."""
    parts = code.split("_")
    # 'dal_cattle'      -> ['dal', 'cattle']        -> level 1
    # 'dal_cattle_beef' -> ['dal', 'cattle', 'beef'] -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_ag_livestock(conn) -> int:
    """Ingest Agricultural Livestock Category domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_ag_livestock'), and links NAICS 112xxx nodes
    via node_taxonomy_link.

    Returns total livestock node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_ag_livestock",
        "Agricultural Livestock Categories",
        "Cattle, swine, poultry, sheep, equine, aquaculture and specialty livestock taxonomy",
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

    parent_codes = {parent for _, _, _, parent in LIVESTOCK_NODES if parent is not None}

    rows = [
        (
            "domain_ag_livestock",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in LIVESTOCK_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(LIVESTOCK_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_ag_livestock'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_ag_livestock'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '112%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_ag_livestock", "primary") for code in naics_codes],
    )

    return count
