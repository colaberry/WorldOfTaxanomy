"""Water and Environment Regulatory Framework Types domain taxonomy ingester.

Classifies water and environmental management by the regulatory framework
that governs permitted activities, discharge limits, and compliance obligations.
Orthogonal to water type/use (domain_water_env). Used by environmental
compliance managers, permitting consultants, water utilities, and
industrial facility operators.

Code prefix: dwreg_
System ID: domain_water_regulatory
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
WATER_REGULATORY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dwreg_us", "United States Water Regulations", 1, None),
    ("dwreg_us_cwa", "Clean Water Act (CWA) - NPDES permits, Section 404", 2, "dwreg_us"),
    ("dwreg_us_sdwa", "Safe Drinking Water Act (SDWA) - MCL standards", 2, "dwreg_us"),
    ("dwreg_us_czma", "Coastal Zone Management Act (CZMA) federal-state", 2, "dwreg_us"),
    ("dwreg_us_cercla", "CERCLA/Superfund - hazardous substance cleanup", 2, "dwreg_us"),
    ("dwreg_eu", "European Union Water Regulations", 1, None),
    ("dwreg_eu_wfd", "EU Water Framework Directive (2000/60/EC) - river basin mgmt", 2, "dwreg_eu"),
    ("dwreg_eu_drinkingwater", "EU Drinking Water Directive (2020/2184/EU)", 2, "dwreg_eu"),
    ("dwreg_eu_marine", "EU Marine Strategy Framework Directive (2008/56/EC)", 2, "dwreg_eu"),
    ("dwreg_eu_floods", "EU Floods Directive (2007/60/EC)", 2, "dwreg_eu"),
    ("dwreg_intl", "International Water and Environmental Frameworks", 1, None),
    ("dwreg_intl_ramsar", "Ramsar Convention on Wetlands of International Importance", 2, "dwreg_intl"),
    ("dwreg_intl_marpol", "MARPOL Convention (marine pollution from ships)", 2, "dwreg_intl"),
    ("dwreg_intl_un2030", "UN SDG 6 (Clean Water and Sanitation) country commitments", 2, "dwreg_intl"),
    ("dwreg_sector", "Sector-Specific Environmental Permits", 1, None),
    ("dwreg_sector_mining", "Mining wastewater and tailings discharge permits", 2, "dwreg_sector"),
    ("dwreg_sector_agri", "Agricultural runoff, nutrient management plans", 2, "dwreg_sector"),
    ("dwreg_sector_munic", "Municipal combined/separate stormwater permits", 2, "dwreg_sector"),
    ("dwreg_sector_oil", "Oil and gas produced water disposal and reuse permits", 2, "dwreg_sector"),
]

_DOMAIN_ROW = (
    "domain_water_regulatory",
    "Water and Environment Regulatory Framework Types",
    "Major water and environmental regulatory regimes: Clean Water Act, EU WFD, SDWA and equivalents",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['2213', '5413', '9241']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_water_regulatory(conn) -> int:
    """Ingest Water and Environment Regulatory Framework Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_water_regulatory'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_water_regulatory",
        "Water and Environment Regulatory Framework Types",
        "Major water and environmental regulatory regimes: Clean Water Act, EU WFD, SDWA and equivalents",
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

    parent_codes = {parent for _, _, _, parent in WATER_REGULATORY_NODES if parent is not None}

    rows = [
        (
            "domain_water_regulatory",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in WATER_REGULATORY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(WATER_REGULATORY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_water_regulatory'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_water_regulatory'",
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
            [("naics_2022", code, "domain_water_regulatory", "primary") for code in naics_codes],
        )

    return count
