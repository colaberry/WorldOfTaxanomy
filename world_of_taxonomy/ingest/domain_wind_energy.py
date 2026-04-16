"""Ingest Wind Energy Technology and Application Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_wind_energy",
    "Wind Energy Types",
    "Wind Energy Technology and Application Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("wind_tech", "Wind Technology", 1, None),
    ("wind_app", "Applications", 1, None),
    ("wind_comp", "Components", 1, None),
    ("wind_tech_hawt", "Horizontal-axis wind turbine (HAWT)", 2, "wind_tech"),
    ("wind_tech_vawt", "Vertical-axis wind turbine (VAWT)", 2, "wind_tech"),
    ("wind_tech_off_fb", "Offshore fixed-bottom", 2, "wind_tech"),
    ("wind_tech_off_fl", "Offshore floating", 2, "wind_tech"),
    ("wind_app_util", "Utility-scale wind farm", 2, "wind_app"),
    ("wind_app_dist", "Distributed/community wind", 2, "wind_app"),
    ("wind_app_small", "Small wind (residential, farm)", 2, "wind_app"),
    ("wind_app_hybrid", "Hybrid wind-solar projects", 2, "wind_app"),
    ("wind_comp_blade", "Blades and rotor systems", 2, "wind_comp"),
    ("wind_comp_tower", "Tower structures", 2, "wind_comp"),
    ("wind_comp_nacelle", "Nacelle and drivetrain", 2, "wind_comp"),
    ("wind_comp_found", "Foundations (monopile, jacket, floating)", 2, "wind_comp"),
    ("wind_comp_sub", "Subsea cables and interconnection", 2, "wind_comp"),
    ("wind_comp_ops", "O&M and service vessels", 2, "wind_comp"),
]


async def ingest_domain_wind_energy(conn) -> int:
    """Insert or update Wind Energy Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_wind_energy"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_wind_energy", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_wind_energy",
    )
    return count
