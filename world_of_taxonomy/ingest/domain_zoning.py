"""Ingest Land Use Zoning Classification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_zoning",
    "Zoning Classification Types",
    "Land Use Zoning Classification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("zn_res", "Residential Zoning", 1, None),
    ("zn_comm", "Commercial Zoning", 1, None),
    ("zn_ind", "Industrial Zoning", 1, None),
    ("zn_spec", "Special Purpose Zoning", 1, None),
    ("zn_res_sf", "Single-family residential (R-1, R-2)", 2, "zn_res"),
    ("zn_res_mf", "Multi-family residential (R-3, R-4)", 2, "zn_res"),
    ("zn_res_mix", "Mixed-use residential", 2, "zn_res"),
    ("zn_res_mob", "Mobile home / manufactured housing", 2, "zn_res"),
    ("zn_comm_nb", "Neighborhood commercial", 2, "zn_comm"),
    ("zn_comm_gen", "General commercial", 2, "zn_comm"),
    ("zn_comm_cbd", "Central business district", 2, "zn_comm"),
    ("zn_comm_highway", "Highway commercial", 2, "zn_comm"),
    ("zn_ind_light", "Light industrial", 2, "zn_ind"),
    ("zn_ind_heavy", "Heavy industrial", 2, "zn_ind"),
    ("zn_ind_bp", "Business park / flex space", 2, "zn_ind"),
    ("zn_spec_ag", "Agricultural zoning", 2, "zn_spec"),
    ("zn_spec_pud", "Planned unit development (PUD)", 2, "zn_spec"),
    ("zn_spec_hist", "Historic preservation overlay", 2, "zn_spec"),
    ("zn_spec_flood", "Flood zone overlay", 2, "zn_spec"),
]


async def ingest_domain_zoning(conn) -> int:
    """Insert or update Zoning Classification Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_zoning"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_zoning", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_zoning",
    )
    return count
