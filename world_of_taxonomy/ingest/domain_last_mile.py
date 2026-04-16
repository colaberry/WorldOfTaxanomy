"""Ingest Last-Mile Delivery and Fulfillment Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_last_mile",
    "Last-Mile Delivery Types",
    "Last-Mile Delivery and Fulfillment Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("lm_mode", "Delivery Modes", 1, None),
    ("lm_model", "Business Models", 1, None),
    ("lm_tech", "Delivery Technology", 1, None),
    ("lm_mode_van", "Van and truck delivery", 2, "lm_mode"),
    ("lm_mode_bike", "Bicycle and e-bike courier", 2, "lm_mode"),
    ("lm_mode_crowd", "Crowdsourced delivery (gig)", 2, "lm_mode"),
    ("lm_mode_locker", "Parcel lockers and pickup points", 2, "lm_mode"),
    ("lm_model_same", "Same-day delivery", 2, "lm_model"),
    ("lm_model_next", "Next-day delivery", 2, "lm_model"),
    ("lm_model_sched", "Scheduled delivery windows", 2, "lm_model"),
    ("lm_model_white", "White glove and installation", 2, "lm_model"),
    ("lm_model_sub", "Subscription delivery", 2, "lm_model"),
    ("lm_tech_drone", "Drone delivery", 2, "lm_tech"),
    ("lm_tech_robot", "Sidewalk delivery robot", 2, "lm_tech"),
    ("lm_tech_auto", "Autonomous delivery vehicle", 2, "lm_tech"),
    ("lm_tech_micro", "Micro-fulfillment center", 2, "lm_tech"),
    ("lm_tech_dark", "Dark store", 2, "lm_tech"),
]


async def ingest_domain_last_mile(conn) -> int:
    """Insert or update Last-Mile Delivery Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_last_mile"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_last_mile", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_last_mile",
    )
    return count
