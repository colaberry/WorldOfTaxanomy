"""Ingest Precision Agriculture Technology Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_precision_ag",
    "Precision Agriculture Types",
    "Precision Agriculture Technology Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pa_sense", "Sensing and Data Collection", 1, None),
    ("pa_anal", "Analytics and Decision Support", 1, None),
    ("pa_act", "Precision Application", 1, None),
    ("pa_sense_sat", "Satellite imagery (multispectral, NDVI)", 2, "pa_sense"),
    ("pa_sense_drone", "Drone and UAV sensing", 2, "pa_sense"),
    ("pa_sense_soil", "In-field soil sensors", 2, "pa_sense"),
    ("pa_sense_weather", "Weather stations and microclimate", 2, "pa_sense"),
    ("pa_sense_yield", "Yield monitoring", 2, "pa_sense"),
    ("pa_anal_vra", "Variable rate application maps", 2, "pa_anal"),
    ("pa_anal_crop", "Crop health modeling", 2, "pa_anal"),
    ("pa_anal_pred", "Predictive analytics (disease, pest, yield)", 2, "pa_anal"),
    ("pa_anal_ai", "AI and machine learning for agriculture", 2, "pa_anal"),
    ("pa_act_vrf", "Variable rate fertilization", 2, "pa_act"),
    ("pa_act_vrs", "Variable rate seeding", 2, "pa_act"),
    ("pa_act_vri", "Variable rate irrigation", 2, "pa_act"),
    ("pa_act_auto", "Auto-steer and guidance systems", 2, "pa_act"),
    ("pa_act_robot", "Agricultural robotics (weeding, harvesting)", 2, "pa_act"),
]


async def ingest_domain_precision_ag(conn) -> int:
    """Insert or update Precision Agriculture Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_precision_ag"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_precision_ag", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_precision_ag",
    )
    return count
