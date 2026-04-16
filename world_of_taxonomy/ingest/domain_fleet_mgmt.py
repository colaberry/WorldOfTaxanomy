"""Ingest Fleet Management Service and Technology Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_fleet_mgmt",
    "Fleet Management Types",
    "Fleet Management Service and Technology Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("fm_svc", "Fleet Services", 1, None),
    ("fm_tech", "Fleet Technology", 1, None),
    ("fm_seg", "Fleet Segments", 1, None),
    ("fm_svc_leas", "Fleet leasing and financing", 2, "fm_svc"),
    ("fm_svc_maint", "Fleet maintenance management", 2, "fm_svc"),
    ("fm_svc_fuel", "Fuel management and cards", 2, "fm_svc"),
    ("fm_svc_insur", "Fleet insurance programs", 2, "fm_svc"),
    ("fm_svc_remarket", "Remarketing and disposal", 2, "fm_svc"),
    ("fm_tech_telem", "Telematics and GPS tracking", 2, "fm_tech"),
    ("fm_tech_dash", "Dashcam and video telematics", 2, "fm_tech"),
    ("fm_tech_route", "Route optimization software", 2, "fm_tech"),
    ("fm_tech_eld", "Electronic logging device (ELD)", 2, "fm_tech"),
    ("fm_tech_pred", "Predictive maintenance analytics", 2, "fm_tech"),
    ("fm_seg_light", "Light-duty fleet", 2, "fm_seg"),
    ("fm_seg_medium", "Medium-duty fleet", 2, "fm_seg"),
    ("fm_seg_heavy", "Heavy-duty fleet", 2, "fm_seg"),
    ("fm_seg_spec", "Specialty vehicles (refrigerated, tanker)", 2, "fm_seg"),
    ("fm_seg_ev", "Electric fleet transition", 2, "fm_seg"),
]


async def ingest_domain_fleet_mgmt(conn) -> int:
    """Insert or update Fleet Management Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_fleet_mgmt"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_fleet_mgmt", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_fleet_mgmt",
    )
    return count
