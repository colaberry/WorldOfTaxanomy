"""Ingest Building Commissioning Process Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_commissioning",
    "Commissioning Types",
    "Building Commissioning Process Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cx_type", "Commissioning Types", 1, None),
    ("cx_phase", "Commissioning Phases", 1, None),
    ("cx_sys", "Systems Commissioned", 1, None),
    ("cx_type_new", "New construction commissioning (Cx)", 2, "cx_type"),
    ("cx_type_retro", "Retro-commissioning (RCx)", 2, "cx_type"),
    ("cx_type_recx", "Re-commissioning", 2, "cx_type"),
    ("cx_type_monitor", "Monitoring-based commissioning (MBCx)", 2, "cx_type"),
    ("cx_phase_design", "Design phase review", 2, "cx_phase"),
    ("cx_phase_constr", "Construction verification", 2, "cx_phase"),
    ("cx_phase_func", "Functional performance testing", 2, "cx_phase"),
    ("cx_phase_season", "Seasonal commissioning", 2, "cx_phase"),
    ("cx_phase_occ", "Post-occupancy verification", 2, "cx_phase"),
    ("cx_sys_hvac", "HVAC systems", 2, "cx_sys"),
    ("cx_sys_light", "Lighting controls", 2, "cx_sys"),
    ("cx_sys_fire", "Fire and life safety", 2, "cx_sys"),
    ("cx_sys_plumb", "Plumbing and domestic hot water", 2, "cx_sys"),
    ("cx_sys_envelope", "Building envelope", 2, "cx_sys"),
    ("cx_sys_elec", "Electrical systems", 2, "cx_sys"),
]


async def ingest_domain_commissioning(conn) -> int:
    """Insert or update Commissioning Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_commissioning"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_commissioning", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_commissioning",
    )
    return count
