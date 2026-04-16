"""Fishing and Aquaculture domain taxonomy ingester.

Organizes fishing and aquaculture industry types into categories aligned with
NAICS 1141 (Fishing) and NAICS 1142 (Hunting and Trapping - aquaculture).

Code prefix: dfa_
Categories: marine fishing, freshwater fishing, aquaculture farm,
seafood processing, fish feed and hatchery.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FISHING_AQUA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Marine Fishing --
    ("dfa_marine",              "Marine Fishing",                                    1, None),
    ("dfa_marine_pelagic",      "Pelagic Fishing (tuna, mackerel, herring)",         2, "dfa_marine"),
    ("dfa_marine_demersal",     "Demersal Fishing (cod, haddock, flatfish)",          2, "dfa_marine"),
    ("dfa_marine_shellfish",    "Shellfish Harvesting (shrimp, crab, lobster)",       2, "dfa_marine"),
    ("dfa_marine_artisanal",    "Artisanal and Small-Scale Marine Fishing",           2, "dfa_marine"),

    # -- Freshwater Fishing --
    ("dfa_fresh",               "Freshwater Fishing",                                1, None),
    ("dfa_fresh_river",         "River and Stream Fishing (trout, salmon)",           2, "dfa_fresh"),
    ("dfa_fresh_lake",          "Lake and Reservoir Fishing (bass, walleye, perch)",  2, "dfa_fresh"),
    ("dfa_fresh_rec",           "Recreational and Sport Freshwater Fishing",          2, "dfa_fresh"),

    # -- Aquaculture Farm --
    ("dfa_aqua",                "Aquaculture Farm",                                  1, None),
    ("dfa_aqua_finfish",        "Finfish Aquaculture (salmon, tilapia, catfish)",     2, "dfa_aqua"),
    ("dfa_aqua_shellaq",        "Shellfish Aquaculture (oyster, mussel, clam)",       2, "dfa_aqua"),
    ("dfa_aqua_seaweed",        "Seaweed and Algae Cultivation",                     2, "dfa_aqua"),
    ("dfa_aqua_offshore",       "Offshore and Open-Ocean Aquaculture",               2, "dfa_aqua"),
    ("dfa_aqua_ras",            "Recirculating Aquaculture Systems (RAS)",            2, "dfa_aqua"),

    # -- Seafood Processing --
    ("dfa_proc",                "Seafood Processing",                                1, None),
    ("dfa_proc_fillet",         "Fish Filleting and Fresh Packaging",                 2, "dfa_proc"),
    ("dfa_proc_frozen",         "Frozen Seafood Processing and Cold Chain",           2, "dfa_proc"),
    ("dfa_proc_canned",         "Canned and Preserved Seafood",                      2, "dfa_proc"),
    ("dfa_proc_valueadd",       "Value-Added Seafood Products (surimi, fish oil)",   2, "dfa_proc"),

    # -- Fish Feed and Hatchery --
    ("dfa_feed",                "Fish Feed and Hatchery",                            1, None),
    ("dfa_feed_pellet",         "Aquaculture Feed (pellets, nutritional mixes)",      2, "dfa_feed"),
    ("dfa_feed_hatch",          "Fish Hatchery and Fingerling Production",            2, "dfa_feed"),
    ("dfa_feed_biotech",        "Aquaculture Biotechnology and Genetics",             2, "dfa_feed"),
]

_DOMAIN_ROW = (
    "domain_fishing_aqua",
    "Fishing and Aquaculture Types",
    "Marine fishing, freshwater fishing, aquaculture, seafood processing, "
    "fish feed and hatchery domain taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link: 1141 (Fishing), 1142 (Hunting/Trapping - incl aquaculture)
_NAICS_PREFIXES = ["1141", "1142"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_fishing_aqua(conn) -> int:
    """Ingest Fishing and Aquaculture domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_fishing_aqua'), and links NAICS 1141/1142 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_fishing_aqua",
        "Fishing and Aquaculture Types",
        "Marine fishing, freshwater fishing, aquaculture, seafood processing, "
        "fish feed and hatchery domain taxonomy",
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

    parent_codes = {parent for _, _, _, parent in FISHING_AQUA_NODES if parent is not None}

    rows = [
        (
            "domain_fishing_aqua",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in FISHING_AQUA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FISHING_AQUA_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_fishing_aqua'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_fishing_aqua'",
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
            [("naics_2022", code, "domain_fishing_aqua", "primary") for code in naics_codes],
        )

    return count
