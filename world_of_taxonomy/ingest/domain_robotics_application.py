"""Robotics Application Domain Types domain taxonomy ingester.

Classifies robotic systems by their primary application domain.
Orthogonal to robot type and sensing technology (domain_robotics, domain_robotics_sensing).
Based on IFR World Robotics report application taxonomy, EUROP roadmap,
and US Robotics Technology Consortium priorities.
Used by robotics OEMs, systems integrators, market researchers and policy makers.

Code prefix: drbapp_
System ID: domain_robotics_application
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ROBOTICS_APPLICATION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("drbapp_industrial", "Industrial Automation Applications", 1, None),
    ("drbapp_industrial_weld", "Arc welding and spot welding robots (automotive, heavy mfg)", 2, "drbapp_industrial"),
    ("drbapp_industrial_assembly", "Assembly and fastening robots (electronics, automotive)", 2, "drbapp_industrial"),
    ("drbapp_industrial_pick", "Pick-and-place and bin picking (e-commerce, food, pharma)", 2, "drbapp_industrial"),
    ("drbapp_industrial_paint", "Surface finishing and painting (spray, coating, polishing)", 2, "drbapp_industrial"),
    ("drbapp_logistics", "Logistics and Warehouse Automation", 1, None),
    ("drbapp_logistics_amr", "Autonomous Mobile Robots (AMR) for goods-to-person", 2, "drbapp_logistics"),
    ("drbapp_logistics_agv", "Automated Guided Vehicles (AGV) for pallet transport", 2, "drbapp_logistics"),
    ("drbapp_logistics_sortation", "High-speed parcel sorting and conveyor systems", 2, "drbapp_logistics"),
    ("drbapp_agriculture", "Agricultural Robots", 1, None),
    ("drbapp_agriculture_harvest", "Autonomous crop harvesting (strawberry, lettuce, apple pickers)", 2, "drbapp_agriculture"),
    ("drbapp_agriculture_scout", "Field scouting drones and ground robots for crop monitoring", 2, "drbapp_agriculture"),
    ("drbapp_medical", "Medical and Surgical Robots", 1, None),
    ("drbapp_medical_surgical", "Surgical robots (da Vinci, CMR Versius, robotic laparoscopy)", 2, "drbapp_medical"),
    ("drbapp_medical_rehab", "Rehabilitation and assistive exoskeletons (Ekso, ReWalk)", 2, "drbapp_medical"),
    ("drbapp_medical_pharmacy", "Pharmacy dispensing and compounding automation", 2, "drbapp_medical"),
    ("drbapp_service", "Consumer and Professional Service Robots", 1, None),
    ("drbapp_service_cleaning", "Floor cleaning and disinfection robots (iRobot, Gaussian)", 2, "drbapp_service"),
    ("drbapp_service_delivery", "Last-mile delivery robots and drones (Starship, Nuro)", 2, "drbapp_service"),
    ("drbapp_service_inspect", "Inspection and maintenance robots (oil/gas, bridge, infrastructure)", 2, "drbapp_service"),
]

_DOMAIN_ROW = (
    "domain_robotics_application",
    "Robotics Application Domain Types",
    "Robotics and autonomous systems end-use application domain classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3332', '3339', '3599']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_robotics_application(conn) -> int:
    """Ingest Robotics Application Domain Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_robotics_application'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_robotics_application",
        "Robotics Application Domain Types",
        "Robotics and autonomous systems end-use application domain classification",
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

    parent_codes = {parent for _, _, _, parent in ROBOTICS_APPLICATION_NODES if parent is not None}

    rows = [
        (
            "domain_robotics_application",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ROBOTICS_APPLICATION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ROBOTICS_APPLICATION_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_robotics_application'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_robotics_application'",
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
            [("naics_2022", code, "domain_robotics_application", "primary") for code in naics_codes],
        )

    return count
