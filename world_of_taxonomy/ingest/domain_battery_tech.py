"""Ingest Battery Technology and Chemistry Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_battery_tech",
    "Battery Technology Types",
    "Battery Technology and Chemistry Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("bat_chem", "Battery Chemistries", 1, None),
    ("bat_app", "Applications", 1, None),
    ("bat_form", "Form Factors", 1, None),
    ("bat_chem_lfp", "LFP (lithium iron phosphate)", 2, "bat_chem"),
    ("bat_chem_nmc", "NMC (nickel manganese cobalt)", 2, "bat_chem"),
    ("bat_chem_nca", "NCA (nickel cobalt aluminum)", 2, "bat_chem"),
    ("bat_chem_lto", "LTO (lithium titanate)", 2, "bat_chem"),
    ("bat_chem_na", "Sodium-ion", 2, "bat_chem"),
    ("bat_chem_solid", "Solid-state batteries", 2, "bat_chem"),
    ("bat_chem_flow", "Flow batteries (vanadium, zinc-bromine)", 2, "bat_chem"),
    ("bat_app_ev", "Electric vehicle traction battery", 2, "bat_app"),
    ("bat_app_ess", "Grid energy storage system", 2, "bat_app"),
    ("bat_app_port", "Portable electronics", 2, "bat_app"),
    ("bat_app_ups", "UPS and backup power", 2, "bat_app"),
    ("bat_app_micro", "Microgrid storage", 2, "bat_app"),
    ("bat_form_cyl", "Cylindrical cells (18650, 21700, 4680)", 2, "bat_form"),
    ("bat_form_pouch", "Pouch cells", 2, "bat_form"),
    ("bat_form_prism", "Prismatic cells", 2, "bat_form"),
    ("bat_form_blade", "Blade battery", 2, "bat_form"),
]


async def ingest_domain_battery_tech(conn) -> int:
    """Insert or update Battery Technology Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_battery_tech"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_battery_tech", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_battery_tech",
    )
    return count
