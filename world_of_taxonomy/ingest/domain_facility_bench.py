"""Ingest Building Performance Benchmarking Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_facility_bench",
    "Facilities Benchmarking Types",
    "Building Performance Benchmarking Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("fb_metric", "Benchmarking Metrics", 1, None),
    ("fb_tool", "Benchmarking Tools", 1, None),
    ("fb_std", "Standards and Programs", 1, None),
    ("fb_metric_eui", "Energy use intensity (EUI, kBtu/sf)", 2, "fb_metric"),
    ("fb_metric_wui", "Water use intensity (WUI, gal/sf)", 2, "fb_metric"),
    ("fb_metric_carbon", "Carbon intensity (kgCO2e/sf)", 2, "fb_metric"),
    ("fb_metric_occ", "Occupant satisfaction scores", 2, "fb_metric"),
    ("fb_metric_maint", "Maintenance cost per sq ft", 2, "fb_metric"),
    ("fb_tool_star", "ENERGY STAR Portfolio Manager", 2, "fb_tool"),
    ("fb_tool_arc", "ARC (LEED performance platform)", 2, "fb_tool"),
    ("fb_tool_gresb", "GRESB (real estate ESG benchmark)", 2, "fb_tool"),
    ("fb_tool_crrem", "CRREM (carbon risk assessment)", 2, "fb_tool"),
    ("fb_std_ashrae", "ASHRAE building EQ program", 2, "fb_std"),
    ("fb_std_boma", "BOMA Experience Exchange", 2, "fb_std"),
    ("fb_std_disclosure", "Mandatory disclosure laws", 2, "fb_std"),
    ("fb_std_LL84", "NYC Local Law 84/97 benchmarking", 2, "fb_std"),
]


async def ingest_domain_facility_bench(conn) -> int:
    """Insert or update Facilities Benchmarking Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_facility_bench"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_facility_bench", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_facility_bench",
    )
    return count
