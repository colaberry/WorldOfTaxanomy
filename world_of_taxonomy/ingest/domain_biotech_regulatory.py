"""Biotechnology Regulatory Pathway Types domain taxonomy ingester.

Classifies the regulatory pathway a biotech product must navigate to reach market.
Orthogonal to product/technology type (domain_biotech). Used by regulatory affairs
teams, business development, and investors when forecasting approval timelines,
clinical trial costs, and market entry probability.
Key frameworks: FDA NDA/BLA/510k/PMA, EMA MAA/centralized, USDA/EPA ag biotech.

Code prefix: dbtreg_
System ID: domain_biotech_regulatory
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
BIOTECH_REGULATORY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dbtreg_fda", "US FDA Regulatory Pathways", 1, None),
    ("dbtreg_fda_nda", "NDA (New Drug Application) for small-molecule drugs", 2, "dbtreg_fda"),
    ("dbtreg_fda_bla", "BLA (Biologics License Application) for biologics", 2, "dbtreg_fda"),
    ("dbtreg_fda_510k", "510(k) Premarket Notification for medical devices", 2, "dbtreg_fda"),
    ("dbtreg_fda_pma", "PMA (Premarket Approval) for high-risk devices", 2, "dbtreg_fda"),
    ("dbtreg_fda_eua", "EUA (Emergency Use Authorization) pathway", 2, "dbtreg_fda"),
    ("dbtreg_fda_bpci", "Biosimilar pathway (BPCI Act, 351(k) applications)", 2, "dbtreg_fda"),
    ("dbtreg_ema", "European Medicines Agency (EMA) Pathways", 1, None),
    ("dbtreg_ema_central", "EMA Centralized Procedure (MAA, pan-EU approval)", 2, "dbtreg_ema"),
    ("dbtreg_ema_atmp", "EMA ATMP classification (gene therapy, somatic cell, TE)", 2, "dbtreg_ema"),
    ("dbtreg_ema_orphan", "EMA Orphan Medicinal Product designation", 2, "dbtreg_ema"),
    ("dbtreg_agbio", "Agricultural Biotechnology Regulatory Pathways", 1, None),
    ("dbtreg_agbio_usda", "USDA APHIS biotech crop review (7 CFR 340 deregulation)", 2, "dbtreg_agbio"),
    ("dbtreg_agbio_epa", "EPA Biopesticides and PIPS (plant-incorporated protectants)", 2, "dbtreg_agbio"),
    ("dbtreg_agbio_fda", "FDA voluntary consultation for biotech food crops", 2, "dbtreg_agbio"),
    ("dbtreg_advanced", "Advanced Therapy Regulatory Frameworks", 1, None),
    ("dbtreg_advanced_gtx", "Gene therapy RMAT designation and accelerated approval", 2, "dbtreg_advanced"),
    ("dbtreg_advanced_ctx", "Cell therapy (CAR-T, stem cell) regulatory pathway", 2, "dbtreg_advanced"),
    ("dbtreg_advanced_rna", "RNA therapeutics (siRNA, mRNA, ASO) regulatory status", 2, "dbtreg_advanced"),
]

_DOMAIN_ROW = (
    "domain_biotech_regulatory",
    "Biotechnology Regulatory Pathway Types",
    "Biotech and biopharma regulatory pathway classification: FDA, EMA, agricultural and gene therapy pathways",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3254', '5417', '6214']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_biotech_regulatory(conn) -> int:
    """Ingest Biotechnology Regulatory Pathway Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_biotech_regulatory'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_biotech_regulatory",
        "Biotechnology Regulatory Pathway Types",
        "Biotech and biopharma regulatory pathway classification: FDA, EMA, agricultural and gene therapy pathways",
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

    parent_codes = {parent for _, _, _, parent in BIOTECH_REGULATORY_NODES if parent is not None}

    rows = [
        (
            "domain_biotech_regulatory",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in BIOTECH_REGULATORY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(BIOTECH_REGULATORY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_biotech_regulatory'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_biotech_regulatory'",
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
            [("naics_2022", code, "domain_biotech_regulatory", "primary") for code in naics_codes],
        )

    return count
