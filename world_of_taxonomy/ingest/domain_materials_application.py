"""Advanced Materials Application Sector Types domain taxonomy ingester.

Classifies advanced materials by their primary application sector.
Orthogonal to material type (domain_adv_materials) and manufacturing process.
Used by materials science researchers, product managers, market analysts,
and IP/patent teams identifying application-specific IP clusters.

Code prefix: damapp_
System ID: domain_materials_application
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
MATERIALS_APPLICATION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("damapp_aero", "Aerospace and Defence Applications", 1, None),
    ("damapp_aero_struct", "Structural airframe materials (CFRP, Ti alloys, Al-Li)", 2, "damapp_aero"),
    ("damapp_aero_engine", "Turbine and engine materials (CMC, TBC, Ni superalloys)", 2, "damapp_aero"),
    ("damapp_aero_radar", "Radar-absorbing and stealth materials", 2, "damapp_aero"),
    ("damapp_elec", "Electronics and Semiconductor Applications", 1, None),
    ("damapp_elec_substrate", "Substrate materials (sapphire, SiC, GaN-on-Si)", 2, "damapp_elec"),
    ("damapp_elec_dielectric", "Dielectric and gate oxide materials (HfO2, low-k)", 2, "damapp_elec"),
    ("damapp_elec_pcb", "PCB and packaging laminates (Rogers, Taconic RF)", 2, "damapp_elec"),
    ("damapp_medical", "Medical Device and Implant Applications", 1, None),
    ("damapp_medical_implant", "Implant and prosthetics materials (Ti6Al4V, cobalt-chrome, PEEK)", 2, "damapp_medical"),
    ("damapp_medical_drug", "Drug delivery biomaterials (PLGA, hydrogels, liposomes)", 2, "damapp_medical"),
    ("damapp_medical_tissue", "Tissue engineering scaffolds and regenerative biomaterials", 2, "damapp_medical"),
    ("damapp_energy", "Energy Generation and Storage Applications", 1, None),
    ("damapp_energy_solar", "Solar cell materials (perovskite, CIGS, III-V multijunction)", 2, "damapp_energy"),
    ("damapp_energy_battery", "Battery electrode and electrolyte materials", 2, "damapp_energy"),
    ("damapp_energy_h2", "Hydrogen fuel cell and electrolyzer materials", 2, "damapp_energy"),
    ("damapp_auto", "Automotive and Mobility Applications", 1, None),
    ("damapp_auto_body", "Lightweight body-in-white materials (AHSS, CFRP panels)", 2, "damapp_auto"),
    ("damapp_auto_therm", "Thermal management materials (heat spreaders, TIMs)", 2, "damapp_auto"),
    ("damapp_auto_sensor", "Sensor substrates and ADAS sensing materials", 2, "damapp_auto"),
]

_DOMAIN_ROW = (
    "domain_materials_application",
    "Advanced Materials Application Sector Types",
    "End-use application sector classification for advanced materials: aerospace, electronics, medical, energy, automotive",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3354', '3369', '3391', '3364']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_materials_application(conn) -> int:
    """Ingest Advanced Materials Application Sector Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_materials_application'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_materials_application",
        "Advanced Materials Application Sector Types",
        "End-use application sector classification for advanced materials: aerospace, electronics, medical, energy, automotive",
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

    parent_codes = {parent for _, _, _, parent in MATERIALS_APPLICATION_NODES if parent is not None}

    rows = [
        (
            "domain_materials_application",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in MATERIALS_APPLICATION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(MATERIALS_APPLICATION_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_materials_application'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_materials_application'",
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
            [("naics_2022", code, "domain_materials_application", "primary") for code in naics_codes],
        )

    return count
