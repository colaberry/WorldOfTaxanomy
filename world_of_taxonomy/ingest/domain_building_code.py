"""Ingest Building Code and Standard Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_building_code",
    "Building Code Types",
    "Building Code and Standard Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("bc_model", "Model Building Codes", 1, None),
    ("bc_spec", "Specialized Codes", 1, None),
    ("bc_energy", "Energy Codes", 1, None),
    ("bc_model_ibc", "International Building Code (IBC)", 2, "bc_model"),
    ("bc_model_irc", "International Residential Code (IRC)", 2, "bc_model"),
    ("bc_model_iebc", "International Existing Building Code (IEBC)", 2, "bc_model"),
    ("bc_spec_ifc", "International Fire Code (IFC)", 2, "bc_spec"),
    ("bc_spec_imc", "International Mechanical Code (IMC)", 2, "bc_spec"),
    ("bc_spec_ipc", "International Plumbing Code (IPC)", 2, "bc_spec"),
    ("bc_spec_nec", "National Electrical Code (NEC / NFPA 70)", 2, "bc_spec"),
    ("bc_spec_nfpa", "NFPA fire and life safety codes", 2, "bc_spec"),
    ("bc_energy_iecc", "International Energy Conservation Code (IECC)", 2, "bc_energy"),
    ("bc_energy_ashrae90", "ASHRAE 90.1 (commercial energy)", 2, "bc_energy"),
    ("bc_energy_title24", "California Title 24", 2, "bc_energy"),
    ("bc_energy_passive", "Passive House (Passivhaus) standard", 2, "bc_energy"),
    ("bc_energy_zerob", "Zero Energy Building standard", 2, "bc_energy"),
]


async def ingest_domain_building_code(conn) -> int:
    """Insert or update Building Code Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_building_code"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_building_code", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_building_code",
    )
    return count
