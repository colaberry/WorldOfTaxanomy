"""Synthetic Biology Application Sector Types domain taxonomy ingester.

Classifies synthetic biology products and programs by their application sector.
Orthogonal to engineering approach and biosafety level (domain_synbio).
Based on BIO, SynBioBeta, and OECD BioEconomy Framework sector taxonomy.
Used by synbio investors, corporate strategy teams, and regulatory affairs
to understand cross-sector opportunity and regulatory landscape.

Code prefix: dsbapp_
System ID: domain_synbio_application
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SYNBIO_APP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dsbapp_industrial", "Industrial Biotechnology Applications", 1, None),
    ("dsbapp_industrial_ferment", "Precision fermentation (proteins, flavor compounds, biomaterials)", 2, "dsbapp_industrial"),
    ("dsbapp_industrial_biocatal", "Biocatalysis and enzyme engineering (specialty chemicals, pharma intermediates)", 2, "dsbapp_industrial"),
    ("dsbapp_industrial_biofuel", "Advanced biofuels (lignocellulosic ethanol, SAF, algae biodiesel)", 2, "dsbapp_industrial"),
    ("dsbapp_agriculture", "Agricultural Synthetic Biology Applications", 1, None),
    ("dsbapp_agriculture_crop", "Enhanced crop traits (drought tolerance, N-fixation, yield)", 2, "dsbapp_agriculture"),
    ("dsbapp_agriculture_soil", "Soil microbiome engineering (biofertilizers, bioprotectants)", 2, "dsbapp_agriculture"),
    ("dsbapp_agriculture_vet", "Veterinary biologics and animal health applications", 2, "dsbapp_agriculture"),
    ("dsbapp_medical", "Medical and Therapeutic Applications", 1, None),
    ("dsbapp_medical_genethx", "Gene therapy (viral and non-viral delivery, in vivo editing)", 2, "dsbapp_medical"),
    ("dsbapp_medical_cellthx", "Cell therapy (CAR-T, iPSC-derived, NK cell therapies)", 2, "dsbapp_medical"),
    ("dsbapp_medical_diag", "Synthetic biology diagnostics (CRISPR-Dx, paper-based tests)", 2, "dsbapp_medical"),
    ("dsbapp_materials", "Sustainable Materials and Chemicals Applications", 1, None),
    ("dsbapp_materials_bioplastic", "Bioplastics and bio-based polymers (PHA, PLA, spider silk)", 2, "dsbapp_materials"),
    ("dsbapp_materials_biocomposite", "Bio-based composites and construction materials", 2, "dsbapp_materials"),
    ("dsbapp_environ", "Environmental Remediation Applications", 1, None),
    ("dsbapp_environ_biorem", "Bioremediation of contaminated soil and water (engineered bacteria)", 2, "dsbapp_environ"),
    ("dsbapp_environ_biosensor", "Whole-cell biosensors for environmental monitoring", 2, "dsbapp_environ"),
    ("dsbapp_environ_carbon", "Biological carbon capture and sequestration", 2, "dsbapp_environ"),
]

_DOMAIN_ROW = (
    "domain_synbio_application",
    "Synthetic Biology Application Sector Types",
    "Synthetic biology and bioengineering end-application sector classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3254', '5417', '1111']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_synbio_application(conn) -> int:
    """Ingest Synthetic Biology Application Sector Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_synbio_application'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_synbio_application",
        "Synthetic Biology Application Sector Types",
        "Synthetic biology and bioengineering end-application sector classification",
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

    parent_codes = {parent for _, _, _, parent in SYNBIO_APP_NODES if parent is not None}

    rows = [
        (
            "domain_synbio_application",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SYNBIO_APP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SYNBIO_APP_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_synbio_application'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_synbio_application'",
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
            [("naics_2022", code, "domain_synbio_application", "primary") for code in naics_codes],
        )

    return count
