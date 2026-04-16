"""Ingest Autonomous Vehicle Automation Level Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_auto_vehicle_level",
    "Autonomous Vehicle Level Types",
    "Autonomous Vehicle Automation Level Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("avl_sae", "SAE Automation Levels", 1, None),
    ("avl_app", "Application Domains", 1, None),
    ("avl_sae_0", "Level 0 - No automation", 2, "avl_sae"),
    ("avl_sae_1", "Level 1 - Driver assistance", 2, "avl_sae"),
    ("avl_sae_2", "Level 2 - Partial automation", 2, "avl_sae"),
    ("avl_sae_2p", "Level 2+ - Advanced driver assistance", 2, "avl_sae"),
    ("avl_sae_3", "Level 3 - Conditional automation", 2, "avl_sae"),
    ("avl_sae_4", "Level 4 - High automation", 2, "avl_sae"),
    ("avl_sae_5", "Level 5 - Full automation", 2, "avl_sae"),
    ("avl_app_pass", "Passenger vehicles (robotaxi, private)", 2, "avl_app"),
    ("avl_app_truck", "Autonomous trucking (hub-to-hub, last mile)", 2, "avl_app"),
    ("avl_app_deliv", "Delivery robots and drones", 2, "avl_app"),
    ("avl_app_agri", "Agricultural autonomous vehicles", 2, "avl_app"),
    ("avl_app_mine", "Mining autonomous vehicles", 2, "avl_app"),
    ("avl_app_port", "Port and terminal autonomous vehicles", 2, "avl_app"),
    ("avl_app_shut", "Autonomous shuttles", 2, "avl_app"),
    ("avl_app_mari", "Autonomous maritime vessels", 2, "avl_app"),
]


async def ingest_domain_auto_vehicle_level(conn) -> int:
    """Insert or update Autonomous Vehicle Level Types system and its nodes. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0, source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance, license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute(
        "DELETE FROM classification_node WHERE system_id = $1", "domain_auto_vehicle_level"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_auto_vehicle_level", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_auto_vehicle_level",
    )
    return count
