"""Ingest Sharing Economy Platform and Model Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_sharing_econ",
    "Sharing Economy Types",
    "Sharing Economy Platform and Model Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("se_type", "Platform Types", 1, None),
    ("se_asset", "Shared Asset Categories", 1, None),
    ("se_model", "Revenue Models", 1, None),
    ("se_type_p2p", "Peer-to-peer marketplace", 2, "se_type"),
    ("se_type_b2c", "B2C sharing platform", 2, "se_type"),
    ("se_type_b2b", "B2B sharing and collaboration", 2, "se_type"),
    ("se_type_coop", "Cooperative platform (platform co-op)", 2, "se_type"),
    ("se_asset_ride", "Ride-sharing and ride-hailing", 2, "se_asset"),
    ("se_asset_accom", "Accommodation sharing (short-term rental)", 2, "se_asset"),
    ("se_asset_space", "Space sharing (office, parking, storage)", 2, "se_asset"),
    ("se_asset_vehicle", "Vehicle sharing (car, bike, scooter)", 2, "se_asset"),
    ("se_asset_equip", "Equipment and tool sharing", 2, "se_asset"),
    ("se_asset_skill", "Skill and labor sharing (freelance, task)", 2, "se_asset"),
    ("se_model_comm", "Commission-based", 2, "se_model"),
    ("se_model_sub", "Subscription-based", 2, "se_model"),
    ("se_model_lead", "Lead generation", 2, "se_model"),
    ("se_model_free", "Freemium with premium features", 2, "se_model"),
]


async def ingest_domain_sharing_econ(conn) -> int:
    """Insert or update Sharing Economy Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_sharing_econ"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_sharing_econ", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_sharing_econ",
    )
    return count
