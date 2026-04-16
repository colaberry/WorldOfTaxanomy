"""Water Ecosystem Service Types domain taxonomy ingester.

Classifies ecosystem services provided by water-dependent natural systems.
Based on TEEB (The Economics of Ecosystems and Biodiversity), CICES v5.1,
and MA (Millennium Ecosystem Assessment) frameworks.
Orthogonal to regulatory category and water type. Used by natural capital
accounting, biodiversity net gain assessment, ESG reporting, and watershed
restoration project scoping.

Code prefix: dweco_
System ID: domain_water_ecosystem
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
WATER_ECOSYSTEM_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dweco_provision", "Provisioning Services", 1, None),
    ("dweco_provision_water", "Fresh water supply for drinking and irrigation", 2, "dweco_provision"),
    ("dweco_provision_food", "Aquatic food production (fish, shellfish, algae)", 2, "dweco_provision"),
    ("dweco_provision_fiber", "Freshwater fiber and raw materials (reed, timber)", 2, "dweco_provision"),
    ("dweco_regulating", "Regulating Services", 1, None),
    ("dweco_regulating_flood", "Flood attenuation and flow regulation (wetlands, floodplains)", 2, "dweco_regulating"),
    ("dweco_regulating_purif", "Water purification and waste treatment (natural filtration)", 2, "dweco_regulating"),
    ("dweco_regulating_climate", "Local climate regulation via evapotranspiration", 2, "dweco_regulating"),
    ("dweco_regulating_erosion", "Erosion control and sediment retention", 2, "dweco_regulating"),
    ("dweco_regulating_carbon", "Carbon sequestration in wetlands and riparian zones", 2, "dweco_regulating"),
    ("dweco_cultural", "Cultural and Social Services", 1, None),
    ("dweco_cultural_recreation", "Recreation and ecotourism (fishing, swimming, kayaking)", 2, "dweco_cultural"),
    ("dweco_cultural_spiritual", "Spiritual, ceremonial and sacred water sites", 2, "dweco_cultural"),
    ("dweco_cultural_aesthetic", "Aesthetic and landscape values", 2, "dweco_cultural"),
    ("dweco_supporting", "Supporting and Habitat Services", 1, None),
    ("dweco_supporting_habitat", "Aquatic and riparian biodiversity habitat", 2, "dweco_supporting"),
    ("dweco_supporting_nutrient", "Nutrient cycling and biogeochemical processing", 2, "dweco_supporting"),
    ("dweco_supporting_seed", "Seed dispersal and genetic flow via waterways", 2, "dweco_supporting"),
]

_DOMAIN_ROW = (
    "domain_water_ecosystem",
    "Water Ecosystem Service Types",
    "Natural capital and ecosystem services classification for freshwater, coastal and terrestrial water systems",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['1131', '1132', '2213', '9241']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_water_ecosystem(conn) -> int:
    """Ingest Water Ecosystem Service Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_water_ecosystem'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_water_ecosystem",
        "Water Ecosystem Service Types",
        "Natural capital and ecosystem services classification for freshwater, coastal and terrestrial water systems",
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

    parent_codes = {parent for _, _, _, parent in WATER_ECOSYSTEM_NODES if parent is not None}

    rows = [
        (
            "domain_water_ecosystem",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in WATER_ECOSYSTEM_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(WATER_ECOSYSTEM_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_water_ecosystem'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_water_ecosystem'",
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
            [("naics_2022", code, "domain_water_ecosystem", "primary") for code in naics_codes],
        )

    return count
