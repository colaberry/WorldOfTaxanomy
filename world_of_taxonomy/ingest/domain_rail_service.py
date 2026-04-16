"""Ingest Rail Service Classification and Category Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_rail_service",
    "Rail Service Classification Types",
    "Rail Service Classification and Category Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("rail_pass", "Passenger Rail", 1, None),
    ("rail_freight", "Freight Rail", 1, None),
    ("rail_infra", "Rail Infrastructure", 1, None),
    ("rail_pass_hsr", "High-speed rail (>250 km/h)", 2, "rail_pass"),
    ("rail_pass_inter", "Intercity rail", 2, "rail_pass"),
    ("rail_pass_reg", "Regional and commuter rail", 2, "rail_pass"),
    ("rail_pass_metro", "Metro and subway", 2, "rail_pass"),
    ("rail_pass_light", "Light rail and tram", 2, "rail_pass"),
    ("rail_freight_bulk", "Bulk freight (coal, grain, ore)", 2, "rail_freight"),
    ("rail_freight_inter", "Intermodal container freight", 2, "rail_freight"),
    ("rail_freight_auto", "Automotive rail transport", 2, "rail_freight"),
    ("rail_freight_tank", "Tank car (chemicals, petroleum)", 2, "rail_freight"),
    ("rail_freight_ref", "Refrigerated rail", 2, "rail_freight"),
    ("rail_infra_track", "Track and right-of-way", 2, "rail_infra"),
    ("rail_infra_signal", "Signaling and train control", 2, "rail_infra"),
    ("rail_infra_elec", "Electrification systems", 2, "rail_infra"),
    ("rail_infra_yard", "Classification yards and terminals", 2, "rail_infra"),
]


async def ingest_domain_rail_service(conn) -> int:
    """Insert or update Rail Service Classification Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_rail_service"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_rail_service", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_rail_service",
    )
    return count
