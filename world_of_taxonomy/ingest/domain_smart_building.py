"""Ingest Smart Building Technology Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_smart_building",
    "Smart Building Types",
    "Smart Building Technology Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sb_sys", "Building Systems", 1, None),
    ("sb_iot", "IoT and Sensors", 1, None),
    ("sb_plat", "Platforms and Analytics", 1, None),
    ("sb_sys_bas", "Building automation system (BAS/BMS)", 2, "sb_sys"),
    ("sb_sys_light", "Smart lighting (DALI, PoE, occupancy)", 2, "sb_sys"),
    ("sb_sys_access", "Access control (biometric, mobile, card)", 2, "sb_sys"),
    ("sb_sys_elev", "Smart elevator dispatch", 2, "sb_sys"),
    ("sb_iot_occ", "Occupancy and people counting sensors", 2, "sb_iot"),
    ("sb_iot_iaq", "Indoor air quality (IAQ) sensors", 2, "sb_iot"),
    ("sb_iot_energy", "Energy sub-metering", 2, "sb_iot"),
    ("sb_iot_water", "Water leak detection", 2, "sb_iot"),
    ("sb_iot_struct", "Structural health monitoring", 2, "sb_iot"),
    ("sb_plat_digital", "Digital twin platform", 2, "sb_plat"),
    ("sb_plat_fault", "Fault detection and diagnostics (FDD)", 2, "sb_plat"),
    ("sb_plat_energy", "Energy management and optimization", 2, "sb_plat"),
    ("sb_plat_exp", "Tenant experience platform", 2, "sb_plat"),
    ("sb_plat_cyber", "Building cybersecurity", 2, "sb_plat"),
]


async def ingest_domain_smart_building(conn) -> int:
    """Insert or update Smart Building Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_smart_building"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_smart_building", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_smart_building",
    )
    return count
