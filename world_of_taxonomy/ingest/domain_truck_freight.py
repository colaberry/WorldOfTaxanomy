"""Truck Freight Type Domain Taxonomy ingester.

Introduces the domain deep-dive pattern. This taxonomy sits BELOW a single
industry classification node (NAICS 484xxx - Truck Transportation) to model
the internal freight type structure of that industry.

Tables used:
  domain_taxonomy    - registers this domain taxonomy
  classification_node (system_id='domain_truck_freight') - freight type nodes
  node_taxonomy_link - connects NAICS 484xxx nodes to this domain taxonomy

Freight types are organized into 3 top-level categories:
  Mode      (dtf_mode*)   - how freight moves (LTL, FTL, Intermodal, etc.)
  Equipment (dtf_equip*)  - trailer/vehicle type (Dry Van, Flatbed, Reefer, etc.)
  Service   (dtf_svc*)    - service level/geography (Local, OTR, Dedicated, etc.)

Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FREIGHT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # ── Mode category ──
    ("dtf_mode",            "Freight Mode",                              1, None),
    ("dtf_mode_ltl",        "LTL - Less-Than-Truckload",                 2, "dtf_mode"),
    ("dtf_mode_ftl",        "FTL - Full Truckload",                      2, "dtf_mode"),
    ("dtf_mode_intermodal", "Intermodal (container-on-flatcar / COFC)",  2, "dtf_mode"),
    ("dtf_mode_drayage",    "Drayage (port/rail terminal moves)",         2, "dtf_mode"),
    ("dtf_mode_expedited",  "Expedited / Hot Shot",                      2, "dtf_mode"),
    ("dtf_mode_partial",    "Partial Truckload (PTL)",                   2, "dtf_mode"),
    ("dtf_mode_vol",        "Volume LTL (VLTL - 6+ pallets, not FTL)",  2, "dtf_mode"),

    # ── Equipment category ──
    ("dtf_equip",           "Equipment Type",                            1, None),
    ("dtf_equip_dryvan",    "Dry Van (enclosed 53-foot trailer)",        2, "dtf_equip"),
    ("dtf_equip_flatbed",   "Flatbed",                                   2, "dtf_equip"),
    ("dtf_equip_reefer",    "Refrigerated (Reefer)",                     2, "dtf_equip"),
    ("dtf_equip_tanker",    "Tanker (liquid or dry bulk)",               2, "dtf_equip"),
    ("dtf_equip_lowboy",    "Lowboy / Double-drop",                      2, "dtf_equip"),
    ("dtf_equip_stepdeck",  "Step-deck / Drop-deck",                     2, "dtf_equip"),
    ("dtf_equip_rgn",       "Removable Gooseneck (RGN / RGN extendable)", 2, "dtf_equip"),
    ("dtf_equip_conestoga", "Conestoga / Rolling Tarp System",           2, "dtf_equip"),
    ("dtf_equip_curtain",   "Curtain-side (Curtainsider)",               2, "dtf_equip"),
    ("dtf_equip_maxtube",   "Max Cube / Turnpike Double",                2, "dtf_equip"),
    ("dtf_equip_auto",      "Auto Carrier / Car Hauler",                 2, "dtf_equip"),
    ("dtf_equip_livestock", "Livestock Trailer",                         2, "dtf_equip"),
    ("dtf_equip_container", "Container Chassis (ISO 20/40/45 ft)",       2, "dtf_equip"),
    ("dtf_equip_straight",  "Straight Truck / Box Truck",                2, "dtf_equip"),
    ("dtf_equip_sprinter",  "Sprinter Van / Cargo Van",                  2, "dtf_equip"),

    # ── Service / Geography category ──
    ("dtf_svc",             "Service Level and Geography",               1, None),
    ("dtf_svc_local",       "Local / Pickup and Delivery (P&D, <100 mi)", 2, "dtf_svc"),
    ("dtf_svc_regional",    "Regional (100-500 mi, same-day or next-day)", 2, "dtf_svc"),
    ("dtf_svc_longhaul",    "Long-haul (>500 mi, 2+ days)",              2, "dtf_svc"),
    ("dtf_svc_otr",         "Over-the-Road (OTR, cross-country)",        2, "dtf_svc"),
    ("dtf_svc_dedicated",   "Dedicated Contract Carriage (DCC)",         2, "dtf_svc"),
    ("dtf_svc_private",     "Private Fleet Operations",                  2, "dtf_svc"),
    ("dtf_svc_owner_op",    "Owner-Operator / Independent Contractor",   2, "dtf_svc"),
    ("dtf_svc_crossborder", "Cross-border (US-Mexico, US-Canada)",       2, "dtf_svc"),

    # ── Cargo Handling category ──
    ("dtf_cargo",           "Cargo Handling Type",                       1, None),
    ("dtf_cargo_general",   "General Freight (mixed commodity)",         2, "dtf_cargo"),
    ("dtf_cargo_hazmat",    "Hazardous Materials (DOT Classes 1-9)",     2, "dtf_cargo"),
    ("dtf_cargo_perishable", "Perishables (temperature-controlled)",     2, "dtf_cargo"),
    ("dtf_cargo_od",        "Overdimensional / Oversize / Overweight",   2, "dtf_cargo"),
    ("dtf_cargo_highvalue", "High-value / Security Cargo",               2, "dtf_cargo"),
    ("dtf_cargo_bulk",      "Bulk Commodity (grain, sand, coal)",        2, "dtf_cargo"),
    ("dtf_cargo_auto",      "Finished Vehicles (automobiles)",           2, "dtf_cargo"),
    ("dtf_cargo_flatbed_gen", "Machinery / Steel / Construction Material", 2, "dtf_cargo"),
    ("dtf_cargo_pharma",    "Pharmaceuticals / Life Sciences",           2, "dtf_cargo"),
    ("dtf_cargo_retail",    "Retail / Consumer Goods",                   2, "dtf_cargo"),
]

_DOMAIN_ROW = (
    "domain_truck_freight",
    "Truck Freight Types",
    "Freight mode, equipment, service level and cargo type taxonomy for truck transportation",
    "WorldOfTaxonomy",
    None,   # url
)

# NAICS codes to link (484xxx = Truck Transportation)
_NAICS_PREFIXES = ["484"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific freight types."""
    parts = code.split("_")
    # 'dtf_mode'      -> ['dtf', 'mode']         -> level 1
    # 'dtf_mode_ltl'  -> ['dtf', 'mode', 'ltl']  -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_truck_freight(conn) -> int:
    """Ingest Truck Freight Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_truck_freight'), and links NAICS 484xxx nodes
    via node_taxonomy_link.

    Returns total freight type node count.
    """
    # Register in classification_system (required by FK on classification_node)
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_truck_freight",
        "Truck Freight Types",
        "Freight mode, equipment, service level and cargo type taxonomy for truck transportation",
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
    parent_codes = {parent for _, _, _, parent in FREIGHT_NODES if parent is not None}

    rows = [
        (
            "domain_truck_freight",
            code,
            title,
            level,
            parent,
            code.split("_")[1],        # sector_code = category abbreviation
            code not in parent_codes,  # is_leaf
        )
        for code, title, level, parent in FREIGHT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FREIGHT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 "
        "WHERE id = 'domain_truck_freight'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 "
        "WHERE id = 'domain_truck_freight'",
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
        ("naics_2022", naics_code, "domain_truck_freight", "primary")
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
