"""Biotechnology Business Model Types domain taxonomy ingester.

Classifies biotech companies and programs by their business model and
commercialization strategy. Orthogonal to product type and regulatory pathway.
Used by investors, business development teams, and strategic planners
to assess company stage, monetization horizon and partnership structures.

Code prefix: dbtbiz_
System ID: domain_biotech_business
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
BIOTECH_BUSINESS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dbtbiz_platprod", "Platform vs. Product Model", 1, None),
    ("dbtbiz_platprod_platform", "Technology platform licensing (tools, reagents, enabling tech)", 2, "dbtbiz_platprod"),
    ("dbtbiz_platprod_product", "Direct product development to regulatory approval", 2, "dbtbiz_platprod"),
    ("dbtbiz_platprod_hybrid", "Hybrid: platform + proprietary pipeline", 2, "dbtbiz_platprod"),
    ("dbtbiz_license", "Licensing and Royalty Models", 1, None),
    ("dbtbiz_license_outin", "In-licensing: acquiring rights to external programs", 2, "dbtbiz_license"),
    ("dbtbiz_license_outext", "Out-licensing: granting rights to programs/IP", 2, "dbtbiz_license"),
    ("dbtbiz_license_royalty", "Royalty stream from commercialized discoveries", 2, "dbtbiz_license"),
    ("dbtbiz_license_milestone", "Milestone-based payments from pharma partnerships", 2, "dbtbiz_license"),
    ("dbtbiz_contract", "Contract Research and Manufacturing (CRO/CDO/CMO)", 1, None),
    ("dbtbiz_contract_cro", "CRO (contract research org): preclinical and clinical services", 2, "dbtbiz_contract"),
    ("dbtbiz_contract_cdmo", "CDMO (contract dev/mfg org): process dev and manufacturing", 2, "dbtbiz_contract"),
    ("dbtbiz_contract_cmo", "CMO (contract manufacturing org): commercial scale production", 2, "dbtbiz_contract"),
    ("dbtbiz_integrated", "Fully Integrated Biopharma (FIBCO) Model", 1, None),
    ("dbtbiz_integrated_disc", "Integrated: discovery through commercial launch", 2, "dbtbiz_integrated"),
    ("dbtbiz_integrated_spin", "Corporate spinout with product + platform retained", 2, "dbtbiz_integrated"),
    ("dbtbiz_invest", "Biotech Investment and Financing Models", 1, None),
    ("dbtbiz_invest_vc", "Venture-backed private biotech (Series A-D)", 2, "dbtbiz_invest"),
    ("dbtbiz_invest_public", "Publicly-traded biotech (NYSE, NASDAQ biotechs)", 2, "dbtbiz_invest"),
    ("dbtbiz_invest_foundry", "Biotech foundry / incubator model (Flagship, a16z Bio)", 2, "dbtbiz_invest"),
]

_DOMAIN_ROW = (
    "domain_biotech_business",
    "Biotechnology Business Model Types",
    "Biotech and biopharma business model classification: platform vs product, licensing, CRO/CMO, partnerships",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3254', '5417', '5231']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_biotech_business(conn) -> int:
    """Ingest Biotechnology Business Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_biotech_business'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_biotech_business",
        "Biotechnology Business Model Types",
        "Biotech and biopharma business model classification: platform vs product, licensing, CRO/CMO, partnerships",
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

    parent_codes = {parent for _, _, _, parent in BIOTECH_BUSINESS_NODES if parent is not None}

    rows = [
        (
            "domain_biotech_business",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in BIOTECH_BUSINESS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(BIOTECH_BUSINESS_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_biotech_business'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_biotech_business'",
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
            [("naics_2022", code, "domain_biotech_business", "primary") for code in naics_codes],
        )

    return count
