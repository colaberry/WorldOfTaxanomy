"""Ingest Real Estate Investment Trust (REIT) Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_reit_type",
    "REIT Types",
    "Real Estate Investment Trust (REIT) Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("reit_eq", "Equity REITs", 1, None),
    ("reit_mort", "Mortgage REITs", 1, None),
    ("reit_hybrid", "Hybrid REITs", 1, None),
    ("reit_eq_office", "Office REITs", 2, "reit_eq"),
    ("reit_eq_retail", "Retail REITs (malls, shopping centers)", 2, "reit_eq"),
    ("reit_eq_indust", "Industrial REITs (warehouse, logistics)", 2, "reit_eq"),
    ("reit_eq_resid", "Residential REITs (apartments, SFR)", 2, "reit_eq"),
    ("reit_eq_health", "Healthcare REITs (hospitals, senior housing)", 2, "reit_eq"),
    ("reit_eq_data", "Data center REITs", 2, "reit_eq"),
    ("reit_eq_tower", "Cell tower REITs", 2, "reit_eq"),
    ("reit_eq_self", "Self-storage REITs", 2, "reit_eq"),
    ("reit_eq_hotel", "Hotel and hospitality REITs", 2, "reit_eq"),
    ("reit_eq_timber", "Timber REITs", 2, "reit_eq"),
    ("reit_eq_spec", "Specialty REITs (gaming, billboard, farmland)", 2, "reit_eq"),
    ("reit_mort_agency", "Agency mREITs", 2, "reit_mort"),
    ("reit_mort_non", "Non-agency mREITs", 2, "reit_mort"),
    ("reit_mort_comm", "Commercial mREITs", 2, "reit_mort"),
]


async def ingest_domain_reit_type(conn) -> int:
    """Insert or update REIT Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_reit_type"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_reit_type", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_reit_type",
    )
    return count
