"""Synthetic Biology Biosafety and Containment Level Types domain taxonomy ingester.

Classifies synthetic biology and genetic engineering activities by their
required biosafety containment level.
Based on CDC/NIH Biosafety in Microbiological and Biomedical Laboratories (BMBL 6th ed.),
WHO Laboratory Biosafety Manual, and Cartagena Protocol on Biosafety.
Orthogonal to application sector. Used by biosafety officers, facility planners,
institutional biosafety committees (IBCs), and insurance underwriters.

Code prefix: dsbsaf_
System ID: domain_synbio_biosafety
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SYNBIO_BIOSAFETY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dsbsaf_bsl1", "Biosafety Level 1 (BSL-1) - Minimal Risk", 1, None),
    ("dsbsaf_bsl1_nonpath", "Non-pathogenic, well-characterized organisms (E. coli K12, S. cerevisiae)", 2, "dsbsaf_bsl1"),
    ("dsbsaf_bsl1_gmo", "Low-risk GMO research under contained use (GLM waiver possible)", 2, "dsbsaf_bsl1"),
    ("dsbsaf_bsl2", "Biosafety Level 2 (BSL-2) - Moderate Risk", 1, None),
    ("dsbsaf_bsl2_human", "Human pathogens causing disease but rarely life-threatening (Salmonella, HBV)", 2, "dsbsaf_bsl2"),
    ("dsbsaf_bsl2_lenti", "Lentiviral and retroviral vectors for gene delivery research", 2, "dsbsaf_bsl2"),
    ("dsbsaf_bsl2_adeno", "Adeno-associated virus (AAV) gene therapy vector production", 2, "dsbsaf_bsl2"),
    ("dsbsaf_bsl3", "Biosafety Level 3 (BSL-3) - Serious Risk", 1, None),
    ("dsbsaf_bsl3_endemic", "Indigenous or exotic agents (TB, SARs-CoV-2 isolates, West Nile)", 2, "dsbsaf_bsl3"),
    ("dsbsaf_bsl3_select", "USDA/CDC Select Agent (Tier 1) requiring Federal Select Agent Program registration", 2, "dsbsaf_bsl3"),
    ("dsbsaf_bsl4", "Biosafety Level 4 (BSL-4) - Maximum Containment", 1, None),
    ("dsbsaf_bsl4_exotic", "Exotic life-threatening agents with no vaccine/treatment (Ebola, Marburg, Lassa)", 2, "dsbsaf_bsl4"),
    ("dsbsaf_bsl4_select2", "Select Agent Tier 1 with potential for mass casualties", 2, "dsbsaf_bsl4"),
    ("dsbsaf_dualuse", "Dual-Use Research of Concern (DURC)", 1, None),
    ("dsbsaf_dualuse_gofinc", "Gain-of-function research involving potential pandemic pathogens", 2, "dsbsaf_dualuse"),
    ("dsbsaf_dualuse_nsabb", "NSABB policy review-requiring research (enhanced transmissibility, pathogenicity)", 2, "dsbsaf_dualuse"),
]

_DOMAIN_ROW = (
    "domain_synbio_biosafety",
    "Synthetic Biology Biosafety and Containment Level Types",
    "Biosafety level and biological containment classification for synthetic biology and genetic engineering research",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3254', '6215', '5417']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_synbio_biosafety(conn) -> int:
    """Ingest Synthetic Biology Biosafety and Containment Level Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_synbio_biosafety'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_synbio_biosafety",
        "Synthetic Biology Biosafety and Containment Level Types",
        "Biosafety level and biological containment classification for synthetic biology and genetic engineering research",
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

    parent_codes = {parent for _, _, _, parent in SYNBIO_BIOSAFETY_NODES if parent is not None}

    rows = [
        (
            "domain_synbio_biosafety",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SYNBIO_BIOSAFETY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SYNBIO_BIOSAFETY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_synbio_biosafety'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_synbio_biosafety'",
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
            [("naics_2022", code, "domain_synbio_biosafety", "primary") for code in naics_codes],
        )

    return count
