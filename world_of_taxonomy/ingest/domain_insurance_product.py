"""Insurance Product domain taxonomy ingester.

Organizes insurance product types aligned with NAICS 5241 (Insurance carriers).

Code prefix: dip_
Categories: Life Insurance, Property and Casualty, Health Insurance, Specialty Insurance.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
INSURANCE_PRODUCT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Life Insurance --
    ("dip_life",            "Life Insurance",                                        1, None),
    ("dip_life_term",       "Term Life Insurance (level, decreasing, convertible)",  2, "dip_life"),
    ("dip_life_whole",      "Whole Life Insurance (ordinary, limited pay, single)",  2, "dip_life"),
    ("dip_life_universal",  "Universal Life Insurance (flexible premium, indexed)",  2, "dip_life"),
    ("dip_life_variable",   "Variable Life Insurance (investment-linked)",           2, "dip_life"),
    ("dip_life_annuity",    "Annuity Products (fixed, variable, deferred, immediate)", 2, "dip_life"),

    # -- Property and Casualty --
    ("dip_pc",              "Property and Casualty Insurance",                       1, None),
    ("dip_pc_home",         "Homeowners Insurance (dwelling, contents, liability)",  2, "dip_pc"),
    ("dip_pc_auto",         "Auto Insurance (liability, collision, comprehensive)",  2, "dip_pc"),
    ("dip_pc_comm",         "Commercial Property Insurance (BOP, inland marine)",    2, "dip_pc"),
    ("dip_pc_liability",    "General Liability Insurance (CGL, products liability)", 2, "dip_pc"),
    ("dip_pc_workers",      "Workers Compensation Insurance (statutory, excess)",    2, "dip_pc"),
    ("dip_pc_umbrella",     "Umbrella and Excess Liability Insurance",               2, "dip_pc"),

    # -- Health Insurance --
    ("dip_health",          "Health Insurance",                                      1, None),
    ("dip_health_group",    "Group Health Insurance (employer-sponsored, ERISA)",    2, "dip_health"),
    ("dip_health_indiv",    "Individual Health Insurance (ACA marketplace plans)",   2, "dip_health"),
    ("dip_health_dental",   "Dental and Vision Insurance (standalone, embedded)",    2, "dip_health"),
    ("dip_health_ltc",      "Long-Term Care Insurance (traditional, hybrid)",        2, "dip_health"),
    ("dip_health_disab",    "Disability Insurance (short-term, long-term, own-occ)", 2, "dip_health"),

    # -- Specialty Insurance --
    ("dip_spec",            "Specialty Insurance",                                   1, None),
    ("dip_spec_eando",      "Errors and Omissions Insurance (professional liability)", 2, "dip_spec"),
    ("dip_spec_dando",      "Directors and Officers Insurance (D&O liability)",      2, "dip_spec"),
    ("dip_spec_cyber",      "Cyber Insurance (data breach, network security)",       2, "dip_spec"),
    ("dip_spec_surety",     "Surety Bonds (contract, commercial, fidelity)",         2, "dip_spec"),
    ("dip_spec_marine",     "Marine and Aviation Insurance (hull, cargo, war risk)", 2, "dip_spec"),
]

_DOMAIN_ROW = (
    "domain_insurance_product",
    "Insurance Product Types",
    "Life insurance, property and casualty, health insurance, "
    "and specialty insurance product taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefix: 5241 (Insurance carriers)
_NAICS_PREFIXES = ["5241"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific insurance product types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_insurance_product(conn) -> int:
    """Ingest Insurance Product domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_insurance_product'), and links NAICS 5241 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_insurance_product",
        "Insurance Product Types",
        "Life insurance, property and casualty, health insurance, "
        "and specialty insurance product taxonomy",
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

    parent_codes = {parent for _, _, _, parent in INSURANCE_PRODUCT_NODES if parent is not None}

    rows = [
        (
            "domain_insurance_product",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in INSURANCE_PRODUCT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(INSURANCE_PRODUCT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_insurance_product'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_insurance_product'",
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
            [("naics_2022", code, "domain_insurance_product", "primary") for code in naics_codes],
        )

    return count
