"""PropTech domain taxonomy ingester.

Organizes property technology sector types aligned with
NAICS 5312 (Offices of real estate agents and brokers),
NAICS 5311 (Lessors of real estate).

Code prefix: dpt_
Categories: Property Management Tech, Real Estate Marketplaces,
Construction Tech, Smart Building, Mortgage Tech.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
PROPTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Property Management Tech --
    ("dpt_mgmt",            "Property Management Tech",                              1, None),
    ("dpt_mgmt_tenant",     "Tenant Management and Communication Platforms",         2, "dpt_mgmt"),
    ("dpt_mgmt_lease",      "Lease Administration and Tracking Software",            2, "dpt_mgmt"),
    ("dpt_mgmt_maint",      "Maintenance Request and Work Order Automation",         2, "dpt_mgmt"),
    ("dpt_mgmt_account",    "Property Accounting and Financial Reporting",           2, "dpt_mgmt"),

    # -- Real Estate Marketplaces --
    ("dpt_market",          "Real Estate Marketplaces",                              1, None),
    ("dpt_market_resid",    "Residential Listing and Search Platforms",              2, "dpt_market"),
    ("dpt_market_comm",     "Commercial Real Estate Marketplaces",                   2, "dpt_market"),
    ("dpt_market_auction",  "Real Estate Auction and Bidding Platforms",             2, "dpt_market"),
    ("dpt_market_data",     "Property Data and Valuation Analytics",                 2, "dpt_market"),

    # -- Construction Tech --
    ("dpt_const",           "Construction Tech",                                     1, None),
    ("dpt_const_bim",       "Building Information Modeling (BIM) Software",          2, "dpt_const"),
    ("dpt_const_project",   "Construction Project Management Platforms",             2, "dpt_const"),
    ("dpt_const_drone",     "Drone and Aerial Site Surveying",                       2, "dpt_const"),
    ("dpt_const_modular",   "Modular and Prefabricated Construction Tech",           2, "dpt_const"),
    ("dpt_const_safety",    "Construction Site Safety Monitoring",                   2, "dpt_const"),

    # -- Smart Building --
    ("dpt_smart",           "Smart Building",                                        1, None),
    ("dpt_smart_iot",       "IoT Sensors and Building Automation Systems",           2, "dpt_smart"),
    ("dpt_smart_energy",    "Energy Management and Optimization",                    2, "dpt_smart"),
    ("dpt_smart_access",    "Access Control and Security Systems",                   2, "dpt_smart"),
    ("dpt_smart_twin",      "Digital Twin and Building Simulation",                  2, "dpt_smart"),

    # -- Mortgage Tech --
    ("dpt_mortgage",        "Mortgage Tech",                                         1, None),
    ("dpt_mortgage_orig",   "Digital Mortgage Origination Platforms",                2, "dpt_mortgage"),
    ("dpt_mortgage_under",  "Automated Underwriting and Risk Assessment",            2, "dpt_mortgage"),
    ("dpt_mortgage_close",  "Digital Closing and E-Signature Solutions",             2, "dpt_mortgage"),
    ("dpt_mortgage_serv",   "Mortgage Servicing and Payment Platforms",              2, "dpt_mortgage"),
]

_DOMAIN_ROW = (
    "domain_proptech",
    "PropTech Types",
    "Property technology types covering property management tech, real estate "
    "marketplaces, construction tech, smart building, and mortgage tech taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5312 (RE agents/brokers), 5311 (Lessors of real estate)
_NAICS_PREFIXES = ["5312", "5311"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific PropTech types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_proptech(conn) -> int:
    """Ingest PropTech domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_proptech'), and links NAICS 5312/5311 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_proptech",
        "PropTech Types",
        "Property technology types covering property management tech, real estate "
        "marketplaces, construction tech, smart building, and mortgage tech taxonomy",
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

    parent_codes = {parent for _, _, _, parent in PROPTECH_NODES if parent is not None}

    rows = [
        (
            "domain_proptech",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in PROPTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(PROPTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_proptech'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_proptech'",
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
            [("naics_2022", code, "domain_proptech", "primary") for code in naics_codes],
        )

    return count
