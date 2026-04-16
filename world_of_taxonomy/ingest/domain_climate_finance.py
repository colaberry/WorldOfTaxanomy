"""Climate Finance Instrument Types domain taxonomy ingester.

Classifies financial instruments and investment vehicles used to finance
climate mitigation and adaptation activities.
Orthogonal to climate technology type (domain_climate_tech) and policy mechanism.
Aligned with Climate Bonds Initiative, ICMA Green Bond Principles, TCFD,
Article 6 Paris Agreement carbon market provisions, and EU Taxonomy.
Used by ESG investors, green finance teams, climate funds and DFIs.

Code prefix: dctfin_
System ID: domain_climate_finance
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CLIMATE_FINANCE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dctfin_bonds", "Green and Climate-Linked Debt Instruments", 1, None),
    ("dctfin_bonds_green", "Green bonds (ICMA GBP-aligned use-of-proceeds)", 2, "dctfin_bonds"),
    ("dctfin_bonds_slb", "Sustainability-linked bonds (SLB, KPI-linked coupon step-up)", 2, "dctfin_bonds"),
    ("dctfin_bonds_climate", "Climate Bonds Initiative certified bonds", 2, "dctfin_bonds"),
    ("dctfin_bonds_transition", "Transition finance instruments (fossil fuel phase-out financing)", 2, "dctfin_bonds"),
    ("dctfin_carbon", "Carbon Markets and Credits", 1, None),
    ("dctfin_carbon_vcs", "Voluntary carbon credits (VCM, Verra VCS, Gold Standard)", 2, "dctfin_carbon"),
    ("dctfin_carbon_ets", "Compliance carbon allowances (EU ETS, RGGI, CA-QC cap-and-trade)", 2, "dctfin_carbon"),
    ("dctfin_carbon_art6", "Paris Agreement Article 6 internationally transferred mitigation outcomes", 2, "dctfin_carbon"),
    ("dctfin_equity", "Climate-Focused Equity and Venture", 1, None),
    ("dctfin_equity_vc", "Climate tech venture capital (Breakthrough Energy, 2150, Clean Energy Ventures)", 2, "dctfin_equity"),
    ("dctfin_equity_growth", "Growth equity for scaling climate solutions", 2, "dctfin_equity"),
    ("dctfin_equity_infra", "Listed climate infrastructure equity (yieldcos, clean energy REITs)", 2, "dctfin_equity"),
    ("dctfin_blended", "Blended and Development Finance", 1, None),
    ("dctfin_blended_dfi", "Development finance institution (DFI) concessional loans", 2, "dctfin_blended"),
    ("dctfin_blended_gcf", "Green Climate Fund (GCF) accredited entity grants", 2, "dctfin_blended"),
    ("dctfin_blended_firstloss", "First-loss guarantees and credit enhancements for private mobilization", 2, "dctfin_blended"),
    ("dctfin_insurance", "Climate Risk Insurance and Risk Transfer", 1, None),
    ("dctfin_insurance_para", "Parametric climate insurance (rainfall index, hurricane index)", 2, "dctfin_insurance"),
    ("dctfin_insurance_cat", "Catastrophe bonds (cat bonds) for climate perils", 2, "dctfin_insurance"),
]

_DOMAIN_ROW = (
    "domain_climate_finance",
    "Climate Finance Instrument Types",
    "Climate finance and investment vehicle classification: green bonds, carbon markets, equity, blended finance",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['5221', '5231', '5239']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_climate_finance(conn) -> int:
    """Ingest Climate Finance Instrument Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_climate_finance'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_climate_finance",
        "Climate Finance Instrument Types",
        "Climate finance and investment vehicle classification: green bonds, carbon markets, equity, blended finance",
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

    parent_codes = {parent for _, _, _, parent in CLIMATE_FINANCE_NODES if parent is not None}

    rows = [
        (
            "domain_climate_finance",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CLIMATE_FINANCE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CLIMATE_FINANCE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_climate_finance'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_climate_finance'",
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
            [("naics_2022", code, "domain_climate_finance", "primary") for code in naics_codes],
        )

    return count
