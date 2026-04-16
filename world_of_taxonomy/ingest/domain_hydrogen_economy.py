"""Hydrogen Economy domain taxonomy ingester.

Organizes hydrogen economy industry types into categories aligned with
NAICS 2211 (Electric Power Generation) and NAICS 3259 (Other Chemical
Product Manufacturing).

Code prefix: dhe_
Categories: green hydrogen, blue hydrogen, fuel cells,
hydrogen storage, hydrogen transport.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
HYDROGEN_ECONOMY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Green Hydrogen --
    ("dhe_green",              "Green Hydrogen",                                     1, None),
    ("dhe_green_electro",      "Electrolysis (PEM, alkaline, solid oxide)",           2, "dhe_green"),
    ("dhe_green_solar",        "Solar-Powered Hydrogen Production",                   2, "dhe_green"),
    ("dhe_green_wind",         "Wind-Powered Hydrogen Production",                    2, "dhe_green"),
    ("dhe_green_bio",          "Biomass and Biohydrogen Production",                  2, "dhe_green"),

    # -- Blue Hydrogen --
    ("dhe_blue",               "Blue Hydrogen",                                      1, None),
    ("dhe_blue_smr",           "Steam Methane Reforming with CCS",                    2, "dhe_blue"),
    ("dhe_blue_atr",           "Autothermal Reforming with CCS",                      2, "dhe_blue"),
    ("dhe_blue_capture",       "Carbon Capture and Sequestration for Hydrogen",        2, "dhe_blue"),
    ("dhe_blue_grey",          "Grey Hydrogen (unabated fossil-based)",                2, "dhe_blue"),

    # -- Fuel Cells --
    ("dhe_cell",               "Fuel Cells",                                         1, None),
    ("dhe_cell_pem",           "PEM Fuel Cells (automotive and portable)",             2, "dhe_cell"),
    ("dhe_cell_sofc",          "Solid Oxide Fuel Cells (stationary power)",            2, "dhe_cell"),
    ("dhe_cell_mcfc",          "Molten Carbonate Fuel Cells",                          2, "dhe_cell"),
    ("dhe_cell_stack",         "Fuel Cell Stack Manufacturing and Assembly",           2, "dhe_cell"),

    # -- Hydrogen Storage --
    ("dhe_store",              "Hydrogen Storage",                                   1, None),
    ("dhe_store_compress",     "Compressed Gas Storage (350-700 bar tanks)",           2, "dhe_store"),
    ("dhe_store_liquid",       "Liquid Hydrogen Cryogenic Storage",                    2, "dhe_store"),
    ("dhe_store_metal",        "Metal Hydride and Solid-State Storage",                2, "dhe_store"),
    ("dhe_store_underground",  "Underground Cavern and Salt Dome Storage",             2, "dhe_store"),

    # -- Hydrogen Transport --
    ("dhe_trans",              "Hydrogen Transport",                                 1, None),
    ("dhe_trans_pipeline",     "Dedicated Hydrogen Pipeline Networks",                 2, "dhe_trans"),
    ("dhe_trans_blend",        "Hydrogen Blending in Natural Gas Pipelines",           2, "dhe_trans"),
    ("dhe_trans_truck",        "Tube Trailer and Tanker Truck Delivery",               2, "dhe_trans"),
    ("dhe_trans_ammonia",      "Ammonia Carrier Conversion and Shipping",              2, "dhe_trans"),
    ("dhe_trans_station",      "Hydrogen Refueling Station Infrastructure",            2, "dhe_trans"),
]

_DOMAIN_ROW = (
    "domain_hydrogen_economy",
    "Hydrogen Economy Types",
    "Green hydrogen, blue hydrogen, fuel cells, hydrogen storage, "
    "and hydrogen transport domain taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link: 2211 (Electric Power), 3259 (Other Chemical Mfg)
_NAICS_PREFIXES = ["2211", "3259"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_hydrogen_economy(conn) -> int:
    """Ingest Hydrogen Economy domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_hydrogen_economy'), and links NAICS 2211/3259 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_hydrogen_economy",
        "Hydrogen Economy Types",
        "Green hydrogen, blue hydrogen, fuel cells, hydrogen storage, "
        "and hydrogen transport domain taxonomy",
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

    parent_codes = {parent for _, _, _, parent in HYDROGEN_ECONOMY_NODES if parent is not None}

    rows = [
        (
            "domain_hydrogen_economy",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in HYDROGEN_ECONOMY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(HYDROGEN_ECONOMY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_hydrogen_economy'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_hydrogen_economy'",
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
            [("naics_2022", code, "domain_hydrogen_economy", "primary") for code in naics_codes],
        )

    return count
