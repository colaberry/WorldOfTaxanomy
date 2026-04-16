"""Ingest Building Retro-Commissioning and Optimization Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_retro_cx",
    "Retro-Commissioning Types",
    "Building Retro-Commissioning and Optimization Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("rcx_assess", "Assessment Methods", 1, None),
    ("rcx_measure", "Common Measures", 1, None),
    ("rcx_tool", "Tools and Technology", 1, None),
    ("rcx_assess_trend", "Trend log analysis", 2, "rcx_assess"),
    ("rcx_assess_energy", "Energy benchmarking and baselining", 2, "rcx_assess"),
    ("rcx_assess_func", "Functional testing of existing systems", 2, "rcx_assess"),
    ("rcx_assess_occ", "Occupant comfort surveys", 2, "rcx_assess"),
    ("rcx_measure_sched", "Schedule optimization", 2, "rcx_measure"),
    ("rcx_measure_reset", "Supply air temperature reset", 2, "rcx_measure"),
    ("rcx_measure_econ", "Economizer repair and optimization", 2, "rcx_measure"),
    ("rcx_measure_vfd", "Variable frequency drive (VFD) optimization", 2, "rcx_measure"),
    ("rcx_measure_sensor", "Sensor calibration and replacement", 2, "rcx_measure"),
    ("rcx_tool_bms", "BMS data analytics", 2, "rcx_tool"),
    ("rcx_tool_fdd", "Automated fault detection (AFDD)", 2, "rcx_tool"),
    ("rcx_tool_energy", "Energy modeling software", 2, "rcx_tool"),
    ("rcx_tool_meter", "Portable metering and logging", 2, "rcx_tool"),
]


async def ingest_domain_retro_cx(conn) -> int:
    """Insert or update Retro-Commissioning Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_retro_cx"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_retro_cx", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_retro_cx",
    )
    return count
