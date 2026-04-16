"""Maritime Shipping domain taxonomy ingester.

Organizes maritime shipping sector types aligned with NAICS 4831
(Deep sea, coastal, and Great Lakes water transportation) and
NAICS 4832 (Inland water transportation) covering container shipping,
bulk cargo, tanker operations, port services, and shipbuilding.

Code prefix: dms_
Categories: Container Shipping, Bulk Cargo, Tanker Operations,
Port Services, Shipbuilding.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Container Shipping --
    ("dms_container",           "Container Shipping",                                1, None),
    ("dms_container_liner",     "Liner Shipping and Scheduled Container Services",   2, "dms_container"),
    ("dms_container_feeder",    "Feeder and Short-Sea Container Services",           2, "dms_container"),
    ("dms_container_reefer",    "Refrigerated Container (Reefer) Shipping",          2, "dms_container"),
    ("dms_container_intermod",  "Intermodal Container Logistics",                    2, "dms_container"),

    # -- Bulk Cargo --
    ("dms_bulk",                "Bulk Cargo",                                        1, None),
    ("dms_bulk_dry",            "Dry Bulk Shipping (grain, ore, coal)",              2, "dms_bulk"),
    ("dms_bulk_break",          "Breakbulk and Project Cargo Shipping",              2, "dms_bulk"),
    ("dms_bulk_roro",           "Roll-On/Roll-Off (RoRo) Vehicle Shipping",         2, "dms_bulk"),
    ("dms_bulk_heavy",          "Heavy-Lift and Oversize Cargo Shipping",            2, "dms_bulk"),

    # -- Tanker Operations --
    ("dms_tanker",              "Tanker Operations",                                 1, None),
    ("dms_tanker_crude",        "Crude Oil Tanker Operations (VLCC, Suezmax)",       2, "dms_tanker"),
    ("dms_tanker_product",      "Product and Chemical Tanker Operations",            2, "dms_tanker"),
    ("dms_tanker_lng",          "LNG and LPG Carrier Operations",                   2, "dms_tanker"),
    ("dms_tanker_bunker",       "Bunkering and Marine Fuel Supply",                  2, "dms_tanker"),

    # -- Port Services --
    ("dms_port",                "Port Services",                                     1, None),
    ("dms_port_terminal",       "Container Terminal Operations",                     2, "dms_port"),
    ("dms_port_stevedore",      "Stevedoring and Cargo Handling",                   2, "dms_port"),
    ("dms_port_pilot",          "Pilotage, Towing, and Harbor Services",             2, "dms_port"),
    ("dms_port_freight",        "Port Freight Forwarding and Customs Brokerage",     2, "dms_port"),

    # -- Shipbuilding --
    ("dms_shipbuild",           "Shipbuilding",                                      1, None),
    ("dms_shipbuild_comm",      "Commercial Vessel Construction",                    2, "dms_shipbuild"),
    ("dms_shipbuild_naval",     "Naval and Defense Shipbuilding",                    2, "dms_shipbuild"),
    ("dms_shipbuild_repair",    "Ship Repair and Dry Dock Services",                2, "dms_shipbuild"),
    ("dms_shipbuild_offshore",  "Offshore Platform and Marine Structure Fabrication", 2, "dms_shipbuild"),
]

_DOMAIN_ROW = (
    "domain_maritime_shipping",
    "Maritime Shipping Types",
    "Maritime shipping sector types covering container shipping, bulk cargo, "
    "tanker operations, port services, and shipbuilding",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 4831 (Deep sea/coastal), 4832 (Inland water)
_NAICS_PREFIXES = ["4831", "4832"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific maritime/shipping types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_maritime_shipping(conn) -> int:
    """Ingest Maritime Shipping domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_maritime_shipping'), and links NAICS 4831/4832
    nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_maritime_shipping",
        "Maritime Shipping Types",
        "Maritime shipping sector types covering container shipping, bulk cargo, "
        "tanker operations, port services, and shipbuilding",
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
            "domain_maritime_shipping",
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
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_maritime_shipping'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_maritime_shipping'",
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
            [("naics_2022", code, "domain_maritime_shipping", "primary") for code in naics_codes],
        )

    return count
