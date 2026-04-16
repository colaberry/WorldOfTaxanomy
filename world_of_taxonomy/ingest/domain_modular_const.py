"""Ingest Modular and Off-Site Construction Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_modular_const",
    "Modular Construction Types",
    "Modular and Off-Site Construction Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("mc_type", "Module Types", 1, None),
    ("mc_method", "Construction Methods", 1, None),
    ("mc_app", "Application Types", 1, None),
    ("mc_type_vol", "Volumetric modules (3D pods, rooms)", 2, "mc_type"),
    ("mc_type_panel", "Panelized construction (2D panels)", 2, "mc_type"),
    ("mc_type_hybrid", "Hybrid modular (volumetric + panelized)", 2, "mc_type"),
    ("mc_type_pod", "Bathroom and kitchen pods", 2, "mc_type"),
    ("mc_method_steel", "Steel-framed modules", 2, "mc_method"),
    ("mc_method_timber", "Timber-framed modules", 2, "mc_method"),
    ("mc_method_conc", "Precast concrete modules", 2, "mc_method"),
    ("mc_method_3d", "3D printed construction", 2, "mc_method"),
    ("mc_app_res", "Residential (single-family, multi-family)", 2, "mc_app"),
    ("mc_app_hotel", "Hospitality (hotel rooms, student housing)", 2, "mc_app"),
    ("mc_app_health", "Healthcare (patient rooms, operating theaters)", 2, "mc_app"),
    ("mc_app_data", "Data center modules", 2, "mc_app"),
    ("mc_app_retail", "Retail and pop-up structures", 2, "mc_app"),
]


async def ingest_domain_modular_const(conn) -> int:
    """Insert or update Modular Construction Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_modular_const"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_modular_const", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_modular_const",
    )
    return count
