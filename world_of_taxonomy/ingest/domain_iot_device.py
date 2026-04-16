"""Ingest Internet of Things Device Classification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_iot_device",
    "IoT Device Classification Types",
    "Internet of Things Device Classification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("iot_cons", "Consumer IoT", 1, None),
    ("iot_ind", "Industrial IoT (IIoT)", 1, None),
    ("iot_infra", "IoT Infrastructure", 1, None),
    ("iot_cons_home", "Smart home devices (thermostat, lighting, locks)", 2, "iot_cons"),
    ("iot_cons_wear", "Wearables (fitness, smartwatch, health)", 2, "iot_cons"),
    ("iot_cons_auto", "Connected vehicle systems", 2, "iot_cons"),
    ("iot_cons_voice", "Voice assistants and smart speakers", 2, "iot_cons"),
    ("iot_ind_mfg", "Manufacturing sensors and controllers", 2, "iot_ind"),
    ("iot_ind_agri", "Agricultural IoT (soil, weather, livestock)", 2, "iot_ind"),
    ("iot_ind_energy", "Energy grid IoT (smart meters, SCADA)", 2, "iot_ind"),
    ("iot_ind_health", "Healthcare IoT (remote patient monitoring)", 2, "iot_ind"),
    ("iot_ind_city", "Smart city infrastructure", 2, "iot_ind"),
    ("iot_infra_gate", "IoT gateways and edge devices", 2, "iot_infra"),
    ("iot_infra_plat", "IoT platforms (AWS IoT, Azure IoT)", 2, "iot_infra"),
    ("iot_infra_conn", "Connectivity (LoRaWAN, NB-IoT, 5G, Zigbee)", 2, "iot_infra"),
    ("iot_infra_sec", "IoT security (device identity, firmware)", 2, "iot_infra"),
    ("iot_infra_twin", "Digital twin integration", 2, "iot_infra"),
]


async def ingest_domain_iot_device(conn) -> int:
    """Insert or update IoT Device Classification Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_iot_device"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_iot_device", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_iot_device",
    )
    return count
