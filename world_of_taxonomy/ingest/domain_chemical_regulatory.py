"""Chemical Regulatory Framework Types domain taxonomy ingester.

Classifies chemicals by the regulatory regime that governs their manufacture,
import, use and disposal. Orthogonal to hazard class and chemical type.
Used by EHS compliance, trade compliance, product stewardship teams, and
supply chain managers assessing cross-border regulatory exposure.
Key frameworks: EU REACH, US TSCA, UN GHS, CWC, Montreal Protocol, Basel Convention.

Code prefix: dchreg_
System ID: domain_chemical_regulatory
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CHEMICAL_REGULATORY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dchreg_eu", "European Union Chemical Regulations", 1, None),
    ("dchreg_eu_reach", "REACH (Registration, Evaluation, Authorisation, Restriction)", 2, "dchreg_eu"),
    ("dchreg_eu_clp", "CLP Regulation (EU GHS implementation)", 2, "dchreg_eu"),
    ("dchreg_eu_biocides", "EU Biocidal Products Regulation (BPR)", 2, "dchreg_eu"),
    ("dchreg_eu_pppr", "Plant Protection Products Regulation (PPPR)", 2, "dchreg_eu"),
    ("dchreg_us", "United States Chemical Regulations", 1, None),
    ("dchreg_us_tsca", "TSCA (Toxic Substances Control Act) inventory", 2, "dchreg_us"),
    ("dchreg_us_epcra", "EPCRA (Emergency Planning and Community Right-to-Know Act)", 2, "dchreg_us"),
    ("dchreg_us_rcra", "RCRA (Resource Conservation and Recovery Act) hazardous waste", 2, "dchreg_us"),
    ("dchreg_us_fifra", "FIFRA (Federal Insecticide, Fungicide, Rodenticide Act)", 2, "dchreg_us"),
    ("dchreg_global", "Global and Multilateral Chemical Frameworks", 1, None),
    ("dchreg_global_ghs", "UN GHS (Globally Harmonized System) compliance tier", 2, "dchreg_global"),
    ("dchreg_global_cwc", "Chemical Weapons Convention (CWC) schedule chemicals", 2, "dchreg_global"),
    ("dchreg_global_montreal", "Montreal Protocol ODS (ozone-depleting substances)", 2, "dchreg_global"),
    ("dchreg_global_pops", "Stockholm POPs Convention (persistent organic pollutants)", 2, "dchreg_global"),
    ("dchreg_transport", "Transport Regulations (Dangerous Goods)", 1, None),
    ("dchreg_transport_iata", "IATA DGR (air transport dangerous goods)", 2, "dchreg_transport"),
    ("dchreg_transport_imdg", "IMDG Code (maritime transport dangerous goods)", 2, "dchreg_transport"),
    ("dchreg_transport_adr", "ADR/RID/ADN (European road/rail/inland waterway)", 2, "dchreg_transport"),
    ("dchreg_transport_dot", "US DOT HMR (Hazardous Materials Regulations)", 2, "dchreg_transport"),
]

_DOMAIN_ROW = (
    "domain_chemical_regulatory",
    "Chemical Regulatory Framework Types",
    "Major chemical regulatory regimes worldwide: REACH, TSCA, GHS, controlled substances",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['325', '324', '5413']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_chemical_regulatory(conn) -> int:
    """Ingest Chemical Regulatory Framework Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_chemical_regulatory'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_chemical_regulatory",
        "Chemical Regulatory Framework Types",
        "Major chemical regulatory regimes worldwide: REACH, TSCA, GHS, controlled substances",
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

    parent_codes = {parent for _, _, _, parent in CHEMICAL_REGULATORY_NODES if parent is not None}

    rows = [
        (
            "domain_chemical_regulatory",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CHEMICAL_REGULATORY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CHEMICAL_REGULATORY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_chemical_regulatory'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_chemical_regulatory'",
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
            [("naics_2022", code, "domain_chemical_regulatory", "primary") for code in naics_codes],
        )

    return count
