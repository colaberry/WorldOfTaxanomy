"""Ingest Solar Energy Technology and Application Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_solar_energy",
    "Solar Energy Types",
    "Solar Energy Technology and Application Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sol_tech", "Solar Technology", 1, None),
    ("sol_app", "Applications", 1, None),
    ("sol_biz", "Business Models", 1, None),
    ("sol_tech_mono", "Monocrystalline silicon PV", 2, "sol_tech"),
    ("sol_tech_poly", "Polycrystalline silicon PV", 2, "sol_tech"),
    ("sol_tech_thin", "Thin-film PV (CdTe, CIGS, a-Si)", 2, "sol_tech"),
    ("sol_tech_pero", "Perovskite solar cells", 2, "sol_tech"),
    ("sol_tech_csp", "Concentrated solar power (CSP)", 2, "sol_tech"),
    ("sol_tech_bipv", "Building-integrated PV (BIPV)", 2, "sol_tech"),
    ("sol_app_util", "Utility-scale solar farm", 2, "sol_app"),
    ("sol_app_comm", "Commercial rooftop solar", 2, "sol_app"),
    ("sol_app_res", "Residential rooftop solar", 2, "sol_app"),
    ("sol_app_comm_g", "Community solar (shared)", 2, "sol_app"),
    ("sol_app_float", "Floating solar (floatovoltaics)", 2, "sol_app"),
    ("sol_app_agri", "Agrivoltaics (solar + agriculture)", 2, "sol_app"),
    ("sol_biz_ppa", "Power purchase agreement (PPA)", 2, "sol_biz"),
    ("sol_biz_lease", "Solar lease", 2, "sol_biz"),
    ("sol_biz_own", "Direct ownership", 2, "sol_biz"),
    ("sol_biz_comm_s", "Community solar subscription", 2, "sol_biz"),
]


async def ingest_domain_solar_energy(conn) -> int:
    """Insert or update Solar Energy Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_solar_energy"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_solar_energy", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_solar_energy",
    )
    return count
