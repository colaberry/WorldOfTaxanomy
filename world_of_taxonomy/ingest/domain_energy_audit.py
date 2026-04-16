"""Ingest Building Energy Audit and Assessment Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_energy_audit",
    "Energy Audit Types",
    "Building Energy Audit and Assessment Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ea_level", "Audit Levels", 1, None),
    ("ea_area", "Audit Areas", 1, None),
    ("ea_cert", "Certifications and Standards", 1, None),
    ("ea_level_walk", "Level 1 - Walk-through audit", 2, "ea_level"),
    ("ea_level_standard", "Level 2 - Standard energy audit", 2, "ea_level"),
    ("ea_level_invest", "Level 3 - Investment-grade audit", 2, "ea_level"),
    ("ea_area_envelope", "Building envelope assessment", 2, "ea_area"),
    ("ea_area_hvac", "HVAC system assessment", 2, "ea_area"),
    ("ea_area_light", "Lighting assessment", 2, "ea_area"),
    ("ea_area_plug", "Plug load assessment", 2, "ea_area"),
    ("ea_area_process", "Process energy assessment", 2, "ea_area"),
    ("ea_area_renew", "Renewable energy feasibility", 2, "ea_area"),
    ("ea_cert_leed", "LEED energy credits", 2, "ea_cert"),
    ("ea_cert_star", "ENERGY STAR certification", 2, "ea_cert"),
    ("ea_cert_iso50001", "ISO 50001 energy management", 2, "ea_cert"),
    ("ea_cert_ashrae", "ASHRAE Level I/II/III standards", 2, "ea_cert"),
]


async def ingest_domain_energy_audit(conn) -> int:
    """Insert or update Energy Audit Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_energy_audit"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_energy_audit", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_energy_audit",
    )
    return count
