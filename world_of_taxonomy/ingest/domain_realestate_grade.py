"""Real Estate Property Class and Grade Types domain taxonomy ingester.

Classifies commercial real estate assets by market quality class and investment grade.
Orthogonal to property type and transaction type.
Based on BOMA, NAIOP, and NCREIF property classification standards.
Used by CRE investors, lenders, appraisers, and brokers when pricing assets,
setting cap rate expectations, and structuring debt covenants.

Code prefix: dregrad_
System ID: domain_realestate_grade
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
REALESTATE_GRADE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dregrad_class", "Traditional A/B/C Office and Industrial Class", 1, None),
    ("dregrad_class_a", "Class A: best quality, prime location, institutional-grade", 2, "dregrad_class"),
    ("dregrad_class_b", "Class B: average quality, functional, good but secondary location", 2, "dregrad_class"),
    ("dregrad_class_c", "Class C: below average, dated, limited amenities, value play", 2, "dregrad_class"),
    ("dregrad_trophy", "Trophy and Flagship Asset Classifications", 1, None),
    ("dregrad_trophy_iconic", "Trophy / iconic: landmark buildings, best-in-market", 2, "dregrad_trophy"),
    ("dregrad_trophy_creative", "Creative campus / lifestyle product (innovation district, mixed-use)", 2, "dregrad_trophy"),
    ("dregrad_invest", "Investment Strategy and Risk-Return Grade", 1, None),
    ("dregrad_invest_core", "Core: stabilized, low risk, Class A, low leverage (NCREIF core)", 2, "dregrad_invest"),
    ("dregrad_invest_coreplus", "Core-Plus: mostly stabilized, minor value-add, modest risk", 2, "dregrad_invest"),
    ("dregrad_invest_valueadd", "Value-Add: significant capex/lease-up required, moderate risk", 2, "dregrad_invest"),
    ("dregrad_invest_opport", "Opportunistic: development, distressed, or full-repositioning", 2, "dregrad_invest"),
    ("dregrad_residential", "Residential Property Quality Grade", 1, None),
    ("dregrad_residential_luxury", "Luxury residential (penthouses, branded residences, trophy condos)", 2, "dregrad_residential"),
    ("dregrad_residential_market", "Market-rate workforce housing", 2, "dregrad_residential"),
    ("dregrad_residential_affordable", "Affordable housing (LIHTC, Section 8, income-restricted)", 2, "dregrad_residential"),
]

_DOMAIN_ROW = (
    "domain_realestate_grade",
    "Real Estate Property Class and Grade Types",
    "Commercial real estate property class and investment grade classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['53', '5311', '5312', '5313']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_realestate_grade(conn) -> int:
    """Ingest Real Estate Property Class and Grade Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_realestate_grade'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_realestate_grade",
        "Real Estate Property Class and Grade Types",
        "Commercial real estate property class and investment grade classification",
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

    parent_codes = {parent for _, _, _, parent in REALESTATE_GRADE_NODES if parent is not None}

    rows = [
        (
            "domain_realestate_grade",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in REALESTATE_GRADE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REALESTATE_GRADE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_realestate_grade'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_realestate_grade'",
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
            [("naics_2022", code, "domain_realestate_grade", "primary") for code in naics_codes],
        )

    return count
