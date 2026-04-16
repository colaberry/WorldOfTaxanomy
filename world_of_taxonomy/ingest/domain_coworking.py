"""Ingest Coworking and Flexible Workspace Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_coworking",
    "Coworking Space Types",
    "Coworking and Flexible Workspace Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cow_type", "Space Types", 1, None),
    ("cow_model", "Business Models", 1, None),
    ("cow_amenity", "Amenity Categories", 1, None),
    ("cow_type_hot", "Hot desking", 2, "cow_type"),
    ("cow_type_ded", "Dedicated desk", 2, "cow_type"),
    ("cow_type_priv", "Private office", 2, "cow_type"),
    ("cow_type_team", "Team suite", 2, "cow_type"),
    ("cow_type_virt", "Virtual office", 2, "cow_type"),
    ("cow_type_meet", "Meeting and event space", 2, "cow_type"),
    ("cow_model_mem", "Membership-based", 2, "cow_model"),
    ("cow_model_day", "Day pass", 2, "cow_model"),
    ("cow_model_corp", "Enterprise/corporate flex", 2, "cow_model"),
    ("cow_model_incub", "Incubator and accelerator space", 2, "cow_model"),
    ("cow_amenity_tech", "Technology (WiFi, AV, printing)", 2, "cow_amenity"),
    ("cow_amenity_well", "Wellness (gym, meditation, nap pods)", 2, "cow_amenity"),
    ("cow_amenity_food", "Food and beverage (kitchen, cafe)", 2, "cow_amenity"),
    ("cow_amenity_comm", "Community and networking events", 2, "cow_amenity"),
]


async def ingest_domain_coworking(conn) -> int:
    """Insert or update Coworking Space Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_coworking"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_coworking", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_coworking",
    )
    return count
