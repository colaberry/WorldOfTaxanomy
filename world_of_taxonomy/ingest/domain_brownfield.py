"""Ingest Brownfield Site Classification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_brownfield",
    "Brownfield Types",
    "Brownfield Site Classification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("bf_type", "Site Types", 1, None),
    ("bf_contam", "Contamination Types", 1, None),
    ("bf_redev", "Redevelopment Approaches", 1, None),
    ("bf_type_ind", "Former industrial sites", 2, "bf_type"),
    ("bf_type_gas", "Former gas stations (petroleum)", 2, "bf_type"),
    ("bf_type_dry", "Former dry cleaners (solvents)", 2, "bf_type"),
    ("bf_type_rail", "Rail yard and rail siding sites", 2, "bf_type"),
    ("bf_type_military", "Former military installations", 2, "bf_type"),
    ("bf_contam_petro", "Petroleum hydrocarbons (BTEX, TPH)", 2, "bf_contam"),
    ("bf_contam_chlor", "Chlorinated solvents (TCE, PCE)", 2, "bf_contam"),
    ("bf_contam_metal", "Heavy metals (lead, arsenic, chromium)", 2, "bf_contam"),
    ("bf_contam_pfas", "PFAS (per- and polyfluoroalkyl)", 2, "bf_contam"),
    ("bf_redev_risk", "Risk-based corrective action (RBCA)", 2, "bf_redev"),
    ("bf_redev_cap", "Engineered cap and barrier", 2, "bf_redev"),
    ("bf_redev_mna", "Monitored natural attenuation", 2, "bf_redev"),
    ("bf_redev_inst", "Institutional controls (deed restrictions)", 2, "bf_redev"),
]


async def ingest_domain_brownfield(conn) -> int:
    """Insert or update Brownfield Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_brownfield"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_brownfield", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_brownfield",
    )
    return count
