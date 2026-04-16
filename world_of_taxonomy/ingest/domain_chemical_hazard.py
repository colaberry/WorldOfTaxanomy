"""Chemical Hazard Classification domain taxonomy ingester.

Classifies chemicals by hazard type per the Globally Harmonized System (GHS)
of Classification and Labelling of Chemicals (UN, 2019 revision).
Used by REACH (EU), TSCA (US), OSHA HazCom 2012, and national regulatory bodies
for SDS authoring, label design, transport classification, and risk assessment.
Orthogonal to chemical type (what it is) - this dimension classifies how dangerous it is.

Code prefix: dchhaz_
System ID: domain_chemical_hazard
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CHEMICAL_HAZARD_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dchhaz_phys", "Physical Hazards", 1, None),
    ("dchhaz_phys_expl", "Explosives (Div 1.1-1.6, unstable explosives)", 2, "dchhaz_phys"),
    ("dchhaz_phys_flamliq", "Flammable Liquids (Cat 1-4, flash point ranges)", 2, "dchhaz_phys"),
    ("dchhaz_phys_flamgas", "Flammable Gases (Cat 1-2, chemical instability)", 2, "dchhaz_phys"),
    ("dchhaz_phys_oxid", "Oxidizing Substances and Organic Peroxides", 2, "dchhaz_phys"),
    ("dchhaz_phys_presgas", "Gases Under Pressure (compressed, liquefied, dissolved)", 2, "dchhaz_phys"),
    ("dchhaz_phys_cormet", "Corrosive to Metals (Cat 1)", 2, "dchhaz_phys"),
    ("dchhaz_health", "Health Hazards", 1, None),
    ("dchhaz_health_acutetox", "Acute Toxicity (oral, dermal, inhalation Cat 1-5)", 2, "dchhaz_health"),
    ("dchhaz_health_skincorr", "Skin Corrosion and Irritation (Cat 1A-1C, Cat 2)", 2, "dchhaz_health"),
    ("dchhaz_health_eyeirr", "Serious Eye Damage and Eye Irritation", 2, "dchhaz_health"),
    ("dchhaz_health_sensitize", "Respiratory and Skin Sensitizers (Cat 1, 1A, 1B)", 2, "dchhaz_health"),
    ("dchhaz_health_mutagen", "Germ Cell Mutagenicity (Cat 1A, 1B, 2)", 2, "dchhaz_health"),
    ("dchhaz_health_carcin", "Carcinogenicity (Cat 1A, 1B, 2)", 2, "dchhaz_health"),
    ("dchhaz_health_reptox", "Reproductive Toxicity (Cat 1A, 1B, 2, additional)", 2, "dchhaz_health"),
    ("dchhaz_health_stot", "STOT Single and Repeated Exposure (Cat 1, 2, 3)", 2, "dchhaz_health"),
    ("dchhaz_health_aspirate", "Aspiration Hazard (Cat 1, 2)", 2, "dchhaz_health"),
    ("dchhaz_env", "Environmental Hazards", 1, None),
    ("dchhaz_env_aquacute", "Acute Aquatic Toxicity (Cat 1-3)", 2, "dchhaz_env"),
    ("dchhaz_env_aquachronic", "Chronic Aquatic Toxicity (Cat 1-4)", 2, "dchhaz_env"),
    ("dchhaz_env_ozone", "Hazardous to the Ozone Layer (Cat 1)", 2, "dchhaz_env"),
]

_DOMAIN_ROW = (
    "domain_chemical_hazard",
    "Chemical Hazard Classification",
    "GHS-aligned chemical hazard classification: physical, health and environmental hazards",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['325', '324']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_chemical_hazard(conn) -> int:
    """Ingest Chemical Hazard Classification domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_chemical_hazard'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_chemical_hazard",
        "Chemical Hazard Classification",
        "GHS-aligned chemical hazard classification: physical, health and environmental hazards",
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

    parent_codes = {parent for _, _, _, parent in CHEMICAL_HAZARD_NODES if parent is not None}

    rows = [
        (
            "domain_chemical_hazard",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CHEMICAL_HAZARD_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CHEMICAL_HAZARD_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_chemical_hazard'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_chemical_hazard'",
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
            [("naics_2022", code, "domain_chemical_hazard", "primary") for code in naics_codes],
        )

    return count
