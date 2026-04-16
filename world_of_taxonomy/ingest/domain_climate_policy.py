"""Climate Policy Mechanism Types domain taxonomy ingester.

Classifies government policy mechanisms used to drive emissions reduction
and climate adaptation. Orthogonal to climate technology type and finance instrument.
Based on IPCC AR6 WGIII policy taxonomy, IEA policy tracking, and
World Bank carbon pricing dashboard.
Used by policy analysts, corporate government affairs teams, and climate strategists.

Code prefix: dctpol_
System ID: domain_climate_policy
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CLIMATE_POLICY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dctpol_pricing", "Carbon and GHG Pricing Mechanisms", 1, None),
    ("dctpol_pricing_ets", "Emissions trading systems (ETS/cap-and-trade, EU ETS, RGGI, China ETS)", 2, "dctpol_pricing"),
    ("dctpol_pricing_tax", "Carbon taxes (Sweden, Canada, UK, Singapore, Chile)", 2, "dctpol_pricing"),
    ("dctpol_pricing_cbam", "Carbon border adjustment mechanisms (EU CBAM, similar measures)", 2, "dctpol_pricing"),
    ("dctpol_standards", "Performance and Technology Standards", 1, None),
    ("dctpol_standards_fuel", "Fuel economy / CO2 standards (CAFE, EU fleet CO2, China NEV)", 2, "dctpol_standards"),
    ("dctpol_standards_build", "Building energy codes (EPBD, IECC, Passivhaus certification)", 2, "dctpol_standards"),
    ("dctpol_standards_prod", "Product efficiency standards (MEPS - minimum energy performance)", 2, "dctpol_standards"),
    ("dctpol_portfolio", "Renewable Portfolio and Clean Energy Standards", 1, None),
    ("dctpol_portfolio_rps", "Renewable Portfolio Standard / Renewable Obligation (US states, UK)", 2, "dctpol_portfolio"),
    ("dctpol_portfolio_ces", "Clean Electricity Standard (carbon-free by date)", 2, "dctpol_portfolio"),
    ("dctpol_subsidies", "Subsidies, Tax Credits and Mandates", 1, None),
    ("dctpol_subsidies_itc", "Investment Tax Credit (ITC) and Production Tax Credit (PTC)", 2, "dctpol_subsidies"),
    ("dctpol_subsidies_ira", "IRA clean energy credits (45X, 48C, 30D, AMPC US)", 2, "dctpol_subsidies"),
    ("dctpol_subsidies_ev", "EV purchase incentives and ZEV mandates", 2, "dctpol_subsidies"),
    ("dctpol_subsidies_redd", "REDD+ payments for ecosystem services", 2, "dctpol_subsidies"),
    ("dctpol_disclosure", "Disclosure and Reporting Mandates", 1, None),
    ("dctpol_disclosure_sec", "SEC climate disclosure rule (Scope 1/2/3 material)", 2, "dctpol_disclosure"),
    ("dctpol_disclosure_csrd", "EU CSRD and ESRS E1 sustainability reporting", 2, "dctpol_disclosure"),
    ("dctpol_disclosure_tcfd", "TCFD-aligned voluntary and mandatory disclosure", 2, "dctpol_disclosure"),
]

_DOMAIN_ROW = (
    "domain_climate_policy",
    "Climate Policy Mechanism Types",
    "Climate policy instrument and mechanism classification: carbon pricing, standards, subsidies and mandates",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['9241', '5413', '2211']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_climate_policy(conn) -> int:
    """Ingest Climate Policy Mechanism Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_climate_policy'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_climate_policy",
        "Climate Policy Mechanism Types",
        "Climate policy instrument and mechanism classification: carbon pricing, standards, subsidies and mandates",
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

    parent_codes = {parent for _, _, _, parent in CLIMATE_POLICY_NODES if parent is not None}

    rows = [
        (
            "domain_climate_policy",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CLIMATE_POLICY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CLIMATE_POLICY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_climate_policy'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_climate_policy'",
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
            [("naics_2022", code, "domain_climate_policy", "primary") for code in naics_codes],
        )

    return count
