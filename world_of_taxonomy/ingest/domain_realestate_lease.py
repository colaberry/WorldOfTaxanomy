"""Real Estate Leasing Structure Types domain taxonomy ingester.

Classifies commercial real estate leases by their expense responsibility structure.
Orthogonal to property type, class, and transaction type.
Based on SIOR and BOMA lease structure standards.
Used by CRE attorneys, asset managers, investors, and tenants
when negotiating, underwriting, and comparing lease economics.

Code prefix: dregls_
System ID: domain_realestate_lease
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
REALESTATE_LEASE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dregls_gross", "Gross Lease Structures (Landlord Pays Expenses)", 1, None),
    ("dregls_gross_full", "Full-service gross lease: all operating expenses included in rent", 2, "dregls_gross"),
    ("dregls_gross_modified", "Modified gross: base rent covers most but not all expense categories", 2, "dregls_gross"),
    ("dregls_net", "Net Lease Structures (Tenant Pays Expenses)", 1, None),
    ("dregls_net_single", "Single-net (N): tenant pays property taxes only", 2, "dregls_net"),
    ("dregls_net_double", "Double-net (NN): tenant pays property taxes + insurance", 2, "dregls_net"),
    ("dregls_net_triple", "Triple-net (NNN): tenant pays taxes, insurance, and maintenance", 2, "dregls_net"),
    ("dregls_net_absolute", "Absolute NNN / bond lease: tenant responsible for all costs including roof/structure", 2, "dregls_net"),
    ("dregls_ground", "Ground Lease and Land Lease Structures", 1, None),
    ("dregls_ground_subordinated", "Subordinated ground lease (lender can foreclose on land)", 2, "dregls_ground"),
    ("dregls_ground_unsubordinated", "Unsubordinated ground lease (landlord retains senior position)", 2, "dregls_ground"),
    ("dregls_percentage", "Percentage and Turnover Lease Structures", 1, None),
    ("dregls_percentage_retail", "Retail percentage lease: base rent + % of gross sales above breakpoint", 2, "dregls_percentage"),
    ("dregls_percentage_hybrid", "Hybrid percentage: low base + high overage (pop-up, outlet retail)", 2, "dregls_percentage"),
    ("dregls_special", "Special Lease Structure Types", 1, None),
    ("dregls_special_popup", "Short-term / pop-up lease (days to months, flexible term)", 2, "dregls_special"),
    ("dregls_special_cowork", "Co-working and flex space membership agreements", 2, "dregls_special"),
]

_DOMAIN_ROW = (
    "domain_realestate_lease",
    "Real Estate Leasing Structure Types",
    "Commercial real estate lease structure and expense allocation classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['53', '5311', '5312']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_realestate_lease(conn) -> int:
    """Ingest Real Estate Leasing Structure Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_realestate_lease'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_realestate_lease",
        "Real Estate Leasing Structure Types",
        "Commercial real estate lease structure and expense allocation classification",
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

    parent_codes = {parent for _, _, _, parent in REALESTATE_LEASE_NODES if parent is not None}

    rows = [
        (
            "domain_realestate_lease",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in REALESTATE_LEASE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REALESTATE_LEASE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_realestate_lease'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_realestate_lease'",
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
            [("naics_2022", code, "domain_realestate_lease", "primary") for code in naics_codes],
        )

    return count
