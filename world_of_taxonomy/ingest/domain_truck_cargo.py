"""Truck Cargo Classification Domain Taxonomy ingester.

Cargo taxonomy organizes commercial truck cargo into:
  Commodity   (dtc_com*)  - general NMFC-style commodity groups
  Hazmat      (dtc_haz*)  - DOT hazardous materials classes 1-9
  Handling    (dtc_hdl*)  - special handling requirements
  Regulatory  (dtc_reg*)  - regulatory and permit categories

Source: NMFC commodity code patterns + DOT hazmat classes. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CARGO_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Commodity Groups category --
    ("dtc_com",             "Commodity Groups",                               1, None),
    ("dtc_com_general",     "General Freight (mixed commodities)",            2, "dtc_com"),
    ("dtc_com_food",        "Food and Grocery Products",                      2, "dtc_com"),
    ("dtc_com_beverage",    "Beverages (alcoholic and non-alcoholic)",        2, "dtc_com"),
    ("dtc_com_auto",        "Automotive Parts and Accessories",               2, "dtc_com"),
    ("dtc_com_machinery",   "Industrial Machinery and Equipment",             2, "dtc_com"),
    ("dtc_com_steel",       "Steel, Metal and Raw Materials",                 2, "dtc_com"),
    ("dtc_com_lumber",      "Lumber, Wood and Building Materials",            2, "dtc_com"),
    ("dtc_com_paper",       "Paper, Pulp and Printing Materials",             2, "dtc_com"),
    ("dtc_com_chemicals",   "Non-hazardous Chemicals and Plastics",           2, "dtc_com"),
    ("dtc_com_pharma",      "Pharmaceuticals and Medical Supplies",           2, "dtc_com"),
    ("dtc_com_electronics", "Electronics and Technology Equipment",           2, "dtc_com"),
    ("dtc_com_retail",      "Retail / Consumer Goods",                        2, "dtc_com"),
    ("dtc_com_apparel",     "Apparel and Textiles",                           2, "dtc_com"),
    ("dtc_com_grain",       "Grain, Feed and Agricultural Commodities",       2, "dtc_com"),
    ("dtc_com_livestock",   "Livestock and Live Animals",                     2, "dtc_com"),
    ("dtc_com_vehicle",     "New Vehicles (automobiles, motorcycles)",        2, "dtc_com"),
    ("dtc_com_waste",       "Waste, Recyclables and Scrap",                   2, "dtc_com"),

    # -- Hazardous Materials category (DOT Classes 1-9) --
    ("dtc_haz",             "Hazardous Materials (DOT Classes 1-9)",          1, None),
    ("dtc_haz_1",           "Class 1 - Explosives",                          2, "dtc_haz"),
    ("dtc_haz_2",           "Class 2 - Gases (flammable, non-flammable, toxic)", 2, "dtc_haz"),
    ("dtc_haz_3",           "Class 3 - Flammable Liquids",                   2, "dtc_haz"),
    ("dtc_haz_4",           "Class 4 - Flammable Solids",                    2, "dtc_haz"),
    ("dtc_haz_5",           "Class 5 - Oxidizers and Organic Peroxides",     2, "dtc_haz"),
    ("dtc_haz_6",           "Class 6 - Toxic and Infectious Substances",     2, "dtc_haz"),
    ("dtc_haz_7",           "Class 7 - Radioactive Materials",               2, "dtc_haz"),
    ("dtc_haz_8",           "Class 8 - Corrosives",                          2, "dtc_haz"),
    ("dtc_haz_9",           "Class 9 - Miscellaneous Hazardous Materials",   2, "dtc_haz"),

    # -- Special Handling category --
    ("dtc_hdl",             "Special Handling Requirements",                  1, None),
    ("dtc_hdl_temp",        "Temperature-controlled (refrigerated/frozen)",  2, "dtc_hdl"),
    ("dtc_hdl_fragile",     "Fragile / High-value / White Glove",            2, "dtc_hdl"),
    ("dtc_hdl_od",          "Overdimensional / Oversize / Overweight (OS/OW)", 2, "dtc_hdl"),
    ("dtc_hdl_highsec",     "High-security / Bonded (customs, currency)",    2, "dtc_hdl"),
    ("dtc_hdl_liftgate",    "Liftgate Required (no loading dock)",           2, "dtc_hdl"),
    ("dtc_hdl_tarp",        "Tarping Required (flatbed protection)",         2, "dtc_hdl"),
    ("dtc_hdl_strapping",   "Blocking and Bracing / Chains / Strapping",     2, "dtc_hdl"),
    ("dtc_hdl_flexi",       "Flexitank / Bulk Liquid in Dry Van",            2, "dtc_hdl"),

    # -- Regulatory / Permit category --
    ("dtc_reg",             "Regulatory and Permit Categories",               1, None),
    ("dtc_reg_hazmat_pl",   "Hazmat Placard Required (49 CFR 172)",          2, "dtc_reg"),
    ("dtc_reg_superload",   "Superload Permit (multi-state coordination)",   2, "dtc_reg"),
    ("dtc_reg_pilot",       "Pilot Car / Escort Required",                   2, "dtc_reg"),
    ("dtc_reg_bonded",      "Bonded / In-transit Customs (CBP Form 7512)",   2, "dtc_reg"),
    ("dtc_reg_food_grade",  "Food-grade Compliance (FDA 21 CFR)",            2, "dtc_reg"),
    ("dtc_reg_pharma_gmp",  "Pharmaceutical GMP (FDA 21 CFR Part 211)",      2, "dtc_reg"),
]

_DOMAIN_ROW = (
    "domain_truck_cargo",
    "Truck Cargo Classification",
    "Commodity groups, hazmat classes, special handling and regulatory categories for truck cargo",
    "WorldOfTaxonomy",
    None,   # url
)

# NAICS codes to link (484xxx = Truck Transportation)
_NAICS_PREFIXES = ["484"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific cargo types."""
    parts = code.split("_")
    # 'dtc_com'         -> ['dtc', 'com']           -> level 1
    # 'dtc_com_general' -> ['dtc', 'com', 'general'] -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_truck_cargo(conn) -> int:
    """Ingest Truck Cargo Classification domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_truck_cargo'), and links NAICS 484xxx nodes
    via node_taxonomy_link.

    Returns total cargo node count.
    """
    # Register in classification_system (required by FK on classification_node)
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_truck_cargo",
        "Truck Cargo Classification",
        "Commodity groups, hazmat classes, special handling and regulatory categories for truck cargo",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    # Also register in domain_taxonomy for domain-specific metadata
    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    # Insert into classification_node (same table as all other systems)
    parent_codes = {parent for _, _, _, parent in CARGO_NODES if parent is not None}

    rows = [
        (
            "domain_truck_cargo",
            code,
            title,
            level,
            parent,
            code.split("_")[1],        # sector_code = category abbreviation
            code not in parent_codes,  # is_leaf
        )
        for code, title, level, parent in CARGO_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CARGO_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 "
        "WHERE id = 'domain_truck_cargo'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 "
        "WHERE id = 'domain_truck_cargo'",
        count,
    )

    # Link NAICS 484xxx nodes to this domain taxonomy
    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '484%'"
        )
    ]

    link_rows = [
        ("naics_2022", naics_code, "domain_truck_cargo", "primary")
        for naics_code in naics_codes
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        link_rows,
    )

    return count
