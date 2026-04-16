"""Aviation Service domain taxonomy ingester.

Organizes aviation service sector types aligned with NAICS 4811
(Scheduled air transportation) and NAICS 4812 (Nonscheduled air
transportation) covering commercial airlines, cargo aviation,
general aviation, airport operations, and aircraft MRO.

Code prefix: das_
Categories: Commercial Airlines, Cargo Aviation, General Aviation,
Airport Operations, Aircraft MRO.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Commercial Airlines --
    ("das_airline",            "Commercial Airlines",                                1, None),
    ("das_airline_full",       "Full-Service Network Carriers",                      2, "das_airline"),
    ("das_airline_low",        "Low-Cost and Ultra-Low-Cost Carriers",               2, "das_airline"),
    ("das_airline_regional",   "Regional and Commuter Airlines",                     2, "das_airline"),
    ("das_airline_charter",    "Charter and On-Demand Passenger Flights",            2, "das_airline"),

    # -- Cargo Aviation --
    ("das_cargo",              "Cargo Aviation",                                     1, None),
    ("das_cargo_integrator",   "Integrated Express and Parcel Carriers",             2, "das_cargo"),
    ("das_cargo_freighter",    "Dedicated Freighter Operations",                     2, "das_cargo"),
    ("das_cargo_belly",        "Belly Cargo and Combi Aircraft Services",            2, "das_cargo"),
    ("das_cargo_cold",         "Cold Chain and Perishable Air Freight",              2, "das_cargo"),

    # -- General Aviation --
    ("das_general",            "General Aviation",                                   1, None),
    ("das_general_private",    "Private and Business Jet Operations",                2, "das_general"),
    ("das_general_flight",     "Flight Training and Pilot Education",                2, "das_general"),
    ("das_general_aerial",     "Aerial Survey, Photography, and Mapping",            2, "das_general"),
    ("das_general_agri",       "Agricultural Aviation and Crop Spraying",            2, "das_general"),

    # -- Airport Operations --
    ("das_airport",            "Airport Operations",                                 1, None),
    ("das_airport_ground",     "Ground Handling and Ramp Services",                  2, "das_airport"),
    ("das_airport_terminal",   "Terminal Management and Passenger Services",         2, "das_airport"),
    ("das_airport_atc",        "Air Traffic Control and Navigation Services",        2, "das_airport"),
    ("das_airport_fuel",       "Aviation Fuel Supply and Hydrant Systems",           2, "das_airport"),

    # -- Aircraft MRO --
    ("das_mro",                "Aircraft MRO",                                       1, None),
    ("das_mro_airframe",       "Airframe Heavy Maintenance and Overhaul",            2, "das_mro"),
    ("das_mro_engine",         "Engine Maintenance, Repair, and Overhaul",           2, "das_mro"),
    ("das_mro_component",      "Component and Avionics Repair Services",             2, "das_mro"),
    ("das_mro_line",           "Line Maintenance and Transit Checks",                2, "das_mro"),
]

_DOMAIN_ROW = (
    "domain_aviation_service",
    "Aviation Service Types",
    "Aviation service sector types covering commercial airlines, cargo aviation, "
    "general aviation, airport operations, and aircraft MRO",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 4811 (Scheduled air), 4812 (Nonscheduled air)
_NAICS_PREFIXES = ["4811", "4812"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific aviation service types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_aviation_service(conn) -> int:
    """Ingest Aviation Service domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_aviation_service'), and links NAICS 4811/4812
    nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_aviation_service",
        "Aviation Service Types",
        "Aviation service sector types covering commercial airlines, cargo aviation, "
        "general aviation, airport operations, and aircraft MRO",
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
            "domain_aviation_service",
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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_aviation_service'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_aviation_service'",
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
            [("naics_2022", code, "domain_aviation_service", "primary") for code in naics_codes],
        )

    return count
