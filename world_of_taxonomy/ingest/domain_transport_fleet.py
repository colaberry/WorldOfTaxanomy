"""Transportation Fleet Ownership and Operating Model Types domain taxonomy ingester.

Classifies transportation operators by their fleet ownership and operating model.
Orthogonal to transport mode, service class, and fare/pricing model.
Used by transport investors, fleet finance teams, insurance underwriters,
and logistics network designers evaluating asset-intensity and operating leverage.

Code prefix: dtrnflt_
System ID: domain_transport_fleet
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
TRANSPORT_FLEET_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dtrnflt_ownerop", "Owner-Operator and Independent Contractor Models", 1, None),
    ("dtrnflt_ownerop_solo", "Solo owner-operator (1-3 vehicles, personal investment)", 2, "dtrnflt_ownerop"),
    ("dtrnflt_ownerop_smallfleet", "Small fleet owner (3-20 vehicles, family business)", 2, "dtrnflt_ownerop"),
    ("dtrnflt_fleetop", "Fleet Operator and Carrier-Owned Models", 1, None),
    ("dtrnflt_fleetop_captive", "Captive carrier (private fleet owned by shipper company)", 2, "dtrnflt_fleetop"),
    ("dtrnflt_fleetop_forprofit", "For-profit common carrier fleet (asset-based trucking, airline)", 2, "dtrnflt_fleetop"),
    ("dtrnflt_fleetop_publicfr", "Public freight operator (rail, port authority-owned vessels)", 2, "dtrnflt_fleetop"),
    ("dtrnflt_leasing", "Leasing and Rental Fleet Models", 1, None),
    ("dtrnflt_leasing_operlease", "Operating lease (no ownership risk, off-balance sheet)", 2, "dtrnflt_leasing"),
    ("dtrnflt_leasing_finlease", "Finance lease (effective ownership, on-balance sheet)", 2, "dtrnflt_leasing"),
    ("dtrnflt_leasing_fleet", "Fleet management service (Ryder, Penske - full-service lease)", 2, "dtrnflt_leasing"),
    ("dtrnflt_assetlight", "Asset-Light and Brokerage Models", 1, None),
    ("dtrnflt_assetlight_broker", "Non-asset broker / freight forwarder (no owned fleet)", 2, "dtrnflt_assetlight"),
    ("dtrnflt_assetlight_platform", "Platform / digital freight broker (Convoy, Uber Freight)", 2, "dtrnflt_assetlight"),
    ("dtrnflt_public", "Public and Government Fleet Models", 1, None),
    ("dtrnflt_public_transit", "Public transit authority fleet (municipal bus, metro rolling stock)", 2, "dtrnflt_public"),
    ("dtrnflt_public_school", "School district and government fleet", 2, "dtrnflt_public"),
]

_DOMAIN_ROW = (
    "domain_transport_fleet",
    "Transportation Fleet Ownership and Operating Model Types",
    "Transportation fleet ownership and operating model classification for freight and passenger carriers",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['48', '49', '5321']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_transport_fleet(conn) -> int:
    """Ingest Transportation Fleet Ownership and Operating Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_transport_fleet'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_transport_fleet",
        "Transportation Fleet Ownership and Operating Model Types",
        "Transportation fleet ownership and operating model classification for freight and passenger carriers",
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

    parent_codes = {parent for _, _, _, parent in TRANSPORT_FLEET_NODES if parent is not None}

    rows = [
        (
            "domain_transport_fleet",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in TRANSPORT_FLEET_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(TRANSPORT_FLEET_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_transport_fleet'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_transport_fleet'",
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
            [("naics_2022", code, "domain_transport_fleet", "primary") for code in naics_codes],
        )

    return count
