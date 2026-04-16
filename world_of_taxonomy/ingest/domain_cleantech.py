"""CleanTech domain taxonomy ingester.

Organizes clean technology sector types aligned with
NAICS 5629 (Remediation and other waste management services),
NAICS 3339 (Other general purpose machinery manufacturing).

Code prefix: dcl_
Categories: Carbon Capture, Water Purification Tech,
Circular Economy, Green Chemistry, Environmental Monitoring.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CLEANTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Carbon Capture --
    ("dcl_carbon",            "Carbon Capture",                                        1, None),
    ("dcl_carbon_dac",        "Direct Air Capture (DAC) Systems",                     2, "dcl_carbon"),
    ("dcl_carbon_point",      "Point-Source Carbon Capture (flue gas, industrial)",   2, "dcl_carbon"),
    ("dcl_carbon_storage",    "Carbon Storage and Geological Sequestration",          2, "dcl_carbon"),
    ("dcl_carbon_util",       "Carbon Utilization and Conversion (CCU)",              2, "dcl_carbon"),
    ("dcl_carbon_ocean",      "Ocean-Based Carbon Removal Technologies",              2, "dcl_carbon"),

    # -- Water Purification Tech --
    ("dcl_water",             "Water Purification Tech",                               1, None),
    ("dcl_water_membrane",    "Membrane Filtration and Reverse Osmosis",              2, "dcl_water"),
    ("dcl_water_desal",       "Desalination Technologies",                            2, "dcl_water"),
    ("dcl_water_uv",          "UV and Advanced Oxidation Treatment",                  2, "dcl_water"),
    ("dcl_water_reuse",       "Wastewater Reclamation and Reuse Systems",             2, "dcl_water"),

    # -- Circular Economy --
    ("dcl_circular",          "Circular Economy",                                      1, None),
    ("dcl_circular_recycle",  "Advanced Recycling and Material Recovery",              2, "dcl_circular"),
    ("dcl_circular_remanuf",  "Remanufacturing and Refurbishment Platforms",          2, "dcl_circular"),
    ("dcl_circular_sharing",  "Product-as-a-Service and Sharing Platforms",           2, "dcl_circular"),
    ("dcl_circular_design",   "Design for Disassembly and Lifecycle Tools",           2, "dcl_circular"),

    # -- Green Chemistry --
    ("dcl_greenchem",         "Green Chemistry",                                       1, None),
    ("dcl_greenchem_bio",     "Bio-Based Chemicals and Solvents",                     2, "dcl_greenchem"),
    ("dcl_greenchem_cat",     "Green Catalysis and Process Intensification",          2, "dcl_greenchem"),
    ("dcl_greenchem_plastic", "Biodegradable and Compostable Polymers",               2, "dcl_greenchem"),
    ("dcl_greenchem_toxic",   "Toxic Substance Replacement and Safer Alternatives",   2, "dcl_greenchem"),

    # -- Environmental Monitoring --
    ("dcl_envmon",            "Environmental Monitoring",                               1, None),
    ("dcl_envmon_air",        "Air Quality Monitoring and Emissions Tracking",         2, "dcl_envmon"),
    ("dcl_envmon_water",      "Water Quality Sensor Networks",                         2, "dcl_envmon"),
    ("dcl_envmon_satellite",  "Satellite-Based Environmental Observation",             2, "dcl_envmon"),
    ("dcl_envmon_bio",        "Biodiversity Monitoring and Ecosystem Tracking",        2, "dcl_envmon"),
]

_DOMAIN_ROW = (
    "domain_cleantech",
    "CleanTech Types",
    "Clean technology types covering carbon capture, water purification tech, "
    "circular economy, green chemistry, and environmental monitoring taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5629 (Remediation/waste), 3339 (General purpose machinery)
_NAICS_PREFIXES = ["5629", "3339"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific CleanTech types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_cleantech(conn) -> int:
    """Ingest CleanTech domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_cleantech'), and links NAICS 5629/3339 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_cleantech",
        "CleanTech Types",
        "Clean technology types covering carbon capture, water purification tech, "
        "circular economy, green chemistry, and environmental monitoring taxonomy",
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

    parent_codes = {parent for _, _, _, parent in CLEANTECH_NODES if parent is not None}

    rows = [
        (
            "domain_cleantech",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CLEANTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CLEANTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_cleantech'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_cleantech'",
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
            [("naics_2022", code, "domain_cleantech", "primary") for code in naics_codes],
        )

    return count
