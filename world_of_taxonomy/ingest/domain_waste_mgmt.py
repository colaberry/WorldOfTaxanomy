"""Waste Management domain taxonomy ingester.

Organizes waste management sector types aligned with NAICS 5621
(Waste collection) and NAICS 5622 (Waste treatment and disposal)
covering solid waste collection, recycling, hazardous waste,
wastewater treatment, and waste-to-energy.

Code prefix: dwm_
Categories: Solid Waste Collection, Recycling and Recovery,
Hazardous Waste, Wastewater Treatment, Waste-to-Energy.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Solid Waste Collection --
    ("dwm_solid",             "Solid Waste Collection",                              1, None),
    ("dwm_solid_resi",        "Residential Curbside Waste Collection",               2, "dwm_solid"),
    ("dwm_solid_comm",        "Commercial and Industrial Waste Collection",          2, "dwm_solid"),
    ("dwm_solid_roll",        "Roll-Off Container and Dumpster Services",            2, "dwm_solid"),
    ("dwm_solid_transfer",    "Transfer Station Operations",                         2, "dwm_solid"),
    ("dwm_solid_landfill",    "Landfill Operations and Management",                  2, "dwm_solid"),

    # -- Recycling and Recovery --
    ("dwm_recycle",           "Recycling and Recovery",                              1, None),
    ("dwm_recycle_single",    "Single-Stream Recycling Processing",                  2, "dwm_recycle"),
    ("dwm_recycle_metal",     "Metal and Scrap Recycling",                           2, "dwm_recycle"),
    ("dwm_recycle_ewaste",    "Electronic Waste Recycling and Recovery",             2, "dwm_recycle"),
    ("dwm_recycle_organic",   "Organic Waste Composting and Diversion",              2, "dwm_recycle"),

    # -- Hazardous Waste --
    ("dwm_hazard",            "Hazardous Waste",                                     1, None),
    ("dwm_hazard_chem",       "Chemical Hazardous Waste Treatment",                  2, "dwm_hazard"),
    ("dwm_hazard_medical",    "Medical and Biomedical Waste Disposal",               2, "dwm_hazard"),
    ("dwm_hazard_nuclear",    "Radioactive and Nuclear Waste Management",            2, "dwm_hazard"),
    ("dwm_hazard_remediate",  "Environmental Remediation and Site Cleanup",          2, "dwm_hazard"),

    # -- Wastewater Treatment --
    ("dwm_water",             "Wastewater Treatment",                                1, None),
    ("dwm_water_municipal",   "Municipal Wastewater Treatment Plants",               2, "dwm_water"),
    ("dwm_water_industrial",  "Industrial Wastewater Processing",                    2, "dwm_water"),
    ("dwm_water_sludge",      "Sludge and Biosolids Management",                    2, "dwm_water"),
    ("dwm_water_reuse",       "Water Reclamation and Reuse Systems",                 2, "dwm_water"),

    # -- Waste-to-Energy --
    ("dwm_energy",            "Waste-to-Energy",                                     1, None),
    ("dwm_energy_incinerate", "Waste Incineration with Energy Recovery",             2, "dwm_energy"),
    ("dwm_energy_biogas",     "Anaerobic Digestion and Biogas Generation",           2, "dwm_energy"),
    ("dwm_energy_rdf",        "Refuse-Derived Fuel Production",                      2, "dwm_energy"),
    ("dwm_energy_plasma",     "Plasma Gasification and Pyrolysis",                   2, "dwm_energy"),
]

_DOMAIN_ROW = (
    "domain_waste_mgmt",
    "Waste Management Types",
    "Waste management sector types covering solid waste collection, "
    "recycling, hazardous waste, wastewater treatment, and waste-to-energy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5621 (Waste collection), 5622 (Waste treatment and disposal)
_NAICS_PREFIXES = ["5621", "5622"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific waste management types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_waste_mgmt(conn) -> int:
    """Ingest Waste Management domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_waste_mgmt'), and links NAICS 5621/5622 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_waste_mgmt",
        "Waste Management Types",
        "Waste management sector types covering solid waste collection, "
        "recycling, hazardous waste, wastewater treatment, and waste-to-energy",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in NODES if parent is not None}

    rows = [
        (
            "domain_waste_mgmt",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_waste_mgmt'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_waste_mgmt'",
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
            [("naics_2022", code, "domain_waste_mgmt", "primary") for code in naics_codes],
        )

    return count
