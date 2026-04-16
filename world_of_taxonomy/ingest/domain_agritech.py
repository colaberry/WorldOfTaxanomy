"""AgriTech domain taxonomy ingester.

Organizes agricultural technology sector types aligned with
NAICS 1111 (Oilseed and grain farming),
NAICS 1112 (Vegetable and melon farming).

Code prefix: dat_
Categories: Precision Agriculture, Farm Management Software,
Agricultural Drones, Soil Analytics, Vertical Farming.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
AGRITECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Precision Agriculture --
    ("dat_precision",           "Precision Agriculture",                                1, None),
    ("dat_precision_gps",       "GPS-Guided Machinery and Auto-Steering",              2, "dat_precision"),
    ("dat_precision_vra",       "Variable Rate Application Technology",                2, "dat_precision"),
    ("dat_precision_remote",    "Remote Sensing and Satellite Imagery",                2, "dat_precision"),
    ("dat_precision_yield",     "Yield Mapping and Harvest Monitoring",                2, "dat_precision"),

    # -- Farm Management Software --
    ("dat_fms",                 "Farm Management Software",                             1, None),
    ("dat_fms_erp",             "Farm ERP and Financial Management",                   2, "dat_fms"),
    ("dat_fms_crop",            "Crop Planning and Rotation Software",                 2, "dat_fms"),
    ("dat_fms_livestock",       "Livestock Tracking and Herd Management",              2, "dat_fms"),
    ("dat_fms_compliance",      "Regulatory Compliance and Record Keeping",            2, "dat_fms"),
    ("dat_fms_market",          "Market Price Tracking and Sales Platforms",            2, "dat_fms"),

    # -- Agricultural Drones --
    ("dat_drone",               "Agricultural Drones",                                  1, None),
    ("dat_drone_spray",         "Crop Spraying and Aerial Application Drones",         2, "dat_drone"),
    ("dat_drone_scout",         "Scouting and Field Inspection Drones",                2, "dat_drone"),
    ("dat_drone_mapping",       "Aerial Mapping and Photogrammetry",                   2, "dat_drone"),
    ("dat_drone_seed",          "Drone-Based Seeding and Reforestation",               2, "dat_drone"),

    # -- Soil Analytics --
    ("dat_soil",                "Soil Analytics",                                       1, None),
    ("dat_soil_sensor",         "In-Field Soil Sensor Networks",                       2, "dat_soil"),
    ("dat_soil_testing",        "Lab and Portable Soil Testing Services",              2, "dat_soil"),
    ("dat_soil_carbon",         "Soil Carbon Measurement and Sequestration",           2, "dat_soil"),
    ("dat_soil_moisture",       "Soil Moisture Monitoring and Irrigation Scheduling",  2, "dat_soil"),

    # -- Vertical Farming --
    ("dat_vertical",            "Vertical Farming",                                     1, None),
    ("dat_vertical_hydro",      "Hydroponic Growing Systems",                          2, "dat_vertical"),
    ("dat_vertical_aero",       "Aeroponic Growing Systems",                           2, "dat_vertical"),
    ("dat_vertical_light",      "LED Grow Lighting and Spectrum Optimization",         2, "dat_vertical"),
    ("dat_vertical_climate",    "Climate Control and Environment Automation",          2, "dat_vertical"),
]

_DOMAIN_ROW = (
    "domain_agritech",
    "AgriTech Types",
    "Agricultural technology types covering precision agriculture, farm management "
    "software, agricultural drones, soil analytics, and vertical farming taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 1111 (Oilseed/grain), 1112 (Vegetable/melon)
_NAICS_PREFIXES = ["1111", "1112"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific AgriTech types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_agritech(conn) -> int:
    """Ingest AgriTech domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_agritech'), and links NAICS 1111/1112 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_agritech",
        "AgriTech Types",
        "Agricultural technology types covering precision agriculture, farm management "
        "software, agricultural drones, soil analytics, and vertical farming taxonomy",
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

    parent_codes = {parent for _, _, _, parent in AGRITECH_NODES if parent is not None}

    rows = [
        (
            "domain_agritech",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in AGRITECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(AGRITECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_agritech'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_agritech'",
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
            [("naics_2022", code, "domain_agritech", "primary") for code in naics_codes],
        )

    return count
