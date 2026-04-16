"""Ingest Building Automation and Control System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_building_auto",
    "Building Automation Types",
    "Building Automation and Control System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ba_proto", "Communication Protocols", 1, None),
    ("ba_ctrl", "Control Types", 1, None),
    ("ba_level", "System Levels", 1, None),
    ("ba_proto_bacnet", "BACnet (ASHRAE 135)", 2, "ba_proto"),
    ("ba_proto_modbus", "Modbus (RTU, TCP/IP)", 2, "ba_proto"),
    ("ba_proto_lonworks", "LonWorks", 2, "ba_proto"),
    ("ba_proto_knx", "KNX", 2, "ba_proto"),
    ("ba_proto_mqtt", "MQTT and REST API (IoT protocols)", 2, "ba_proto"),
    ("ba_ctrl_ddc", "Direct digital control (DDC)", 2, "ba_ctrl"),
    ("ba_ctrl_pid", "PID loop control", 2, "ba_ctrl"),
    ("ba_ctrl_seq", "Sequence of operations", 2, "ba_ctrl"),
    ("ba_ctrl_opt", "Optimal start/stop", 2, "ba_ctrl"),
    ("ba_level_field", "Field level (sensors, actuators)", 2, "ba_level"),
    ("ba_level_auto", "Automation level (controllers)", 2, "ba_level"),
    ("ba_level_mgmt", "Management level (BMS workstation)", 2, "ba_level"),
    ("ba_level_enter", "Enterprise level (analytics, cloud)", 2, "ba_level"),
]


async def ingest_domain_building_auto(conn) -> int:
    """Insert or update Building Automation Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_building_auto"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_building_auto", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_building_auto",
    )
    return count
