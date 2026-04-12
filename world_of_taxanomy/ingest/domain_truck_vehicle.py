"""Truck Vehicle Class Domain Taxonomy ingester.

Vehicle Class Taxonomy covers:
  DOT Classes (dtv_dot*)  - GVWR-based DOT weight classes 1-8
  Body Types  (dtv_body*) - specific vehicle body configurations

DOT Class definitions (GVWR ranges):
  Class 1: < 6,000 lbs    (light duty - pickup trucks)
  Class 2: 6,001-10,000   (light duty - larger pickups)
  Class 3: 10,001-14,000  (medium duty - cargo vans, small box trucks)
  Class 4: 14,001-16,000  (medium duty - city delivery)
  Class 5: 16,001-19,500  (medium duty - large walk-in)
  Class 6: 19,501-26,000  (medium duty - single axle straight)
  Class 7: 26,001-33,000  (heavy duty - city transit, tractor)
  Class 8: > 33,000 lbs   (heavy duty - semi trucks, heavy straight)

Source: DOT Federal Motor Carrier Safety Administration. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
VEHICLE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- DOT Weight Class category --
    ("dtv_dot",         "DOT GVWR Weight Classes",                       1, None),
    ("dtv_dot_1",       "Class 1 (< 6,000 lbs - light pickup)",          2, "dtv_dot"),
    ("dtv_dot_2",       "Class 2 (6,001-10,000 lbs - larger pickup)",    2, "dtv_dot"),
    ("dtv_dot_3",       "Class 3 (10,001-14,000 lbs - cargo van)",       2, "dtv_dot"),
    ("dtv_dot_4",       "Class 4 (14,001-16,000 lbs - city delivery)",   2, "dtv_dot"),
    ("dtv_dot_5",       "Class 5 (16,001-19,500 lbs - large walk-in)",   2, "dtv_dot"),
    ("dtv_dot_6",       "Class 6 (19,501-26,000 lbs - straight truck)",  2, "dtv_dot"),
    ("dtv_dot_7",       "Class 7 (26,001-33,000 lbs - city transit)",    2, "dtv_dot"),
    ("dtv_dot_8",       "Class 8 (> 33,000 lbs - semi, heavy straight)", 2, "dtv_dot"),

    # -- Body Type category --
    ("dtv_body",        "Vehicle Body Types",                             1, None),
    ("dtv_body_semi",   "Semi-truck / Tractor-trailer (Class 8)",         2, "dtv_body"),
    ("dtv_body_boxtruck", "Box Truck / Straight Truck (Class 3-6)",       2, "dtv_body"),
    ("dtv_body_pickup", "Pickup Truck (Class 1-2)",                       2, "dtv_body"),
    ("dtv_body_cargo_van", "Cargo Van / Sprinter Van (Class 2-3)",        2, "dtv_body"),
    ("dtv_body_flatbed", "Flatbed Truck (Class 5-8)",                     2, "dtv_body"),
    ("dtv_body_tanker", "Tanker Truck (Class 6-8)",                       2, "dtv_body"),
    ("dtv_body_dump",   "Dump Truck (Class 5-8)",                         2, "dtv_body"),
    ("dtv_body_reefer", "Refrigerated Truck / Reefer (Class 5-8)",        2, "dtv_body"),
    ("dtv_body_crane",  "Crane / Boom Truck (Class 6-8)",                 2, "dtv_body"),
    ("dtv_body_lowboy", "Lowboy / Heavy Haul (Class 8)",                  2, "dtv_body"),
    ("dtv_body_auto",   "Auto Transport / Car Hauler (Class 7-8)",        2, "dtv_body"),
    ("dtv_body_wrecker", "Wrecker / Tow Truck (Class 4-8)",              2, "dtv_body"),
    ("dtv_body_mixertruck", "Concrete Mixer Truck (Class 7-8)",          2, "dtv_body"),
]

_DOMAIN_ROW = (
    "domain_truck_vehicle",
    "Truck Vehicle Classes",
    "DOT GVWR weight class and body type taxonomy for commercial trucks",
    "WorldOfTaxanomy",
    None,   # url
)

# NAICS codes to link (484xxx = Truck Transportation)
_NAICS_PREFIXES = ["484"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific vehicle classes/types."""
    parts = code.split("_")
    # 'dtv_dot'   -> ['dtv', 'dot']         -> level 1
    # 'dtv_dot_8' -> ['dtv', 'dot', '8']    -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_truck_vehicle(conn) -> int:
    """Ingest Truck Vehicle Class domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_truck_vehicle'), and links NAICS 484xxx nodes
    via node_taxonomy_link.

    Returns total vehicle class node count.
    """
    # Register in classification_system (required by FK on classification_node)
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_truck_vehicle",
        "Truck Vehicle Classes",
        "DOT GVWR weight class and body type taxonomy for commercial trucks",
        "1.0",
        "United States",
        "WorldOfTaxanomy",
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
    parent_codes = {parent for _, _, _, parent in VEHICLE_NODES if parent is not None}

    rows = [
        (
            "domain_truck_vehicle",
            code,
            title,
            level,
            parent,
            code.split("_")[1],        # sector_code = category abbreviation
            code not in parent_codes,  # is_leaf
        )
        for code, title, level, parent in VEHICLE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(VEHICLE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 "
        "WHERE id = 'domain_truck_vehicle'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 "
        "WHERE id = 'domain_truck_vehicle'",
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
        ("naics_2022", naics_code, "domain_truck_vehicle", "primary")
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
