"""Truck Carrier Operations Domain Taxonomy ingester.

Operations taxonomy covers the carrier operations structure:
  Carrier Type  (dto_type*)   - for-hire, private, owner-operator, broker
  Fleet Size    (dto_fleet*)  - fleet size tiers by power unit count
  Business Model (dto_biz*)  - dispatch model and contract type
  Route         (dto_route*)  - route pattern and geographic scope

Source: FMCSA carrier classification + industry conventions. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
OPS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Carrier Type category --
    ("dto_type",          "Carrier Type",                                   1, None),
    ("dto_type_forhire",  "For-Hire Carrier (transports freight for compensation)", 2, "dto_type"),
    ("dto_type_private",  "Private Carrier (transports own goods)",         2, "dto_type"),
    ("dto_type_owner_op", "Owner-Operator / Independent Contractor",        2, "dto_type"),
    ("dto_type_broker",   "Freight Broker (arranges transport, no truck)",  2, "dto_type"),
    ("dto_type_forwarder", "Freight Forwarder (consolidates/arranges)",     2, "dto_type"),
    ("dto_type_intermodal_mktg", "Intermodal Marketing Company (IMC)",      2, "dto_type"),

    # -- Fleet Size category --
    ("dto_fleet",         "Fleet Size",                                     1, None),
    ("dto_fleet_micro",   "Micro Fleet (1-2 power units)",                  2, "dto_fleet"),
    ("dto_fleet_small",   "Small Fleet (3-20 power units)",                 2, "dto_fleet"),
    ("dto_fleet_mid",     "Mid-size Fleet (21-100 power units)",            2, "dto_fleet"),
    ("dto_fleet_large",   "Large Fleet (101-500 power units)",              2, "dto_fleet"),
    ("dto_fleet_mega",    "Mega Carrier (500+ power units)",                2, "dto_fleet"),

    # -- Business Model category --
    ("dto_biz",           "Business Model",                                 1, None),
    ("dto_biz_dedicated", "Dedicated Contract Carriage (fixed customer)",   2, "dto_biz"),
    ("dto_biz_spot",      "Spot Market / Transactional (load board)",       2, "dto_biz"),
    ("dto_biz_contract",  "Contract Carrier (negotiated rates, set lanes)", 2, "dto_biz"),
    ("dto_biz_lease",     "Power-Only / Leased Operator Program",           2, "dto_biz"),
    ("dto_biz_dispatch",  "Dispatch Service Provider",                      2, "dto_biz"),

    # -- Route Pattern category --
    ("dto_route",         "Route Pattern",                                  1, None),
    ("dto_route_linehaul","Line-Haul (terminal-to-terminal, 500+ mi)",      2, "dto_route"),
    ("dto_route_pd",      "Pickup and Delivery (P&D, local city routes)",   2, "dto_route"),
    ("dto_route_relay",   "Relay / Team Driving (continuous movement)",     2, "dto_route"),
    ("dto_route_milk",    "Milk Run / Multi-stop (scheduled stops)",        2, "dto_route"),
    ("dto_route_backhaul","Backhaul / Return Load",                         2, "dto_route"),
    ("dto_route_drop",    "Drop-and-Hook (no live load/unload)",            2, "dto_route"),
    ("dto_route_live",    "Live Load and Unload",                           2, "dto_route"),
]

_DOMAIN_ROW = (
    "domain_truck_ops",
    "Truck Carrier Operations",
    "Carrier type, fleet size, business model and route pattern taxonomy for truck carrier operations",
    "WorldOfTaxanomy",
    None,   # url
)

# NAICS codes to link (484xxx = Truck Transportation)
_NAICS_PREFIXES = ["484"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific operation types."""
    parts = code.split("_")
    # 'dto_type'         -> ['dto', 'type']             -> level 1
    # 'dto_type_forhire' -> ['dto', 'type', 'forhire']  -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_truck_ops(conn) -> int:
    """Ingest Truck Carrier Operations domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_truck_ops'), and links NAICS 484xxx nodes
    via node_taxonomy_link.

    Returns total operations node count.
    """
    # Register in classification_system (required by FK on classification_node)
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_truck_ops",
        "Truck Carrier Operations",
        "Carrier type, fleet size, business model and route pattern taxonomy for truck carrier operations",
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
    parent_codes = {parent for _, _, _, parent in OPS_NODES if parent is not None}

    rows = [
        (
            "domain_truck_ops",
            code,
            title,
            level,
            parent,
            code.split("_")[1],        # sector_code = category abbreviation
            code not in parent_codes,  # is_leaf
        )
        for code, title, level, parent in OPS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(OPS_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 "
        "WHERE id = 'domain_truck_ops'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 "
        "WHERE id = 'domain_truck_ops'",
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
        ("naics_2022", naics_code, "domain_truck_ops", "primary")
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
