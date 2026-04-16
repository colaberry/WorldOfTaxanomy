"""Ingest Parking Facility and Management Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_parking",
    "Parking Types",
    "Parking Facility and Management Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pk_type", "Parking Types", 1, None),
    ("pk_tech", "Parking Technology", 1, None),
    ("pk_mgmt", "Management Models", 1, None),
    ("pk_type_surface", "Surface lot parking", 2, "pk_type"),
    ("pk_type_struct", "Structured parking garage", 2, "pk_type"),
    ("pk_type_under", "Underground parking", 2, "pk_type"),
    ("pk_type_mech", "Automated / mechanical parking", 2, "pk_type"),
    ("pk_type_on", "On-street metered parking", 2, "pk_type"),
    ("pk_tech_lpr", "License plate recognition (LPR)", 2, "pk_tech"),
    ("pk_tech_sensor", "Occupancy sensors and guidance", 2, "pk_tech"),
    ("pk_tech_app", "Mobile payment and reservation apps", 2, "pk_tech"),
    ("pk_tech_ev", "EV charging integration", 2, "pk_tech"),
    ("pk_mgmt_self", "Self-operated parking", 2, "pk_mgmt"),
    ("pk_mgmt_valet", "Valet parking service", 2, "pk_mgmt"),
    ("pk_mgmt_third", "Third-party management contract", 2, "pk_mgmt"),
    ("pk_mgmt_lease", "Parking lease / concession", 2, "pk_mgmt"),
]


async def ingest_domain_parking(conn) -> int:
    """Insert or update Parking Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_parking"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_parking", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_parking",
    )
    return count
