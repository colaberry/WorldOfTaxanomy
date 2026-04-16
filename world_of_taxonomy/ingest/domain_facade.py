"""Ingest Building Facade and Envelope System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_facade",
    "Facade System Types",
    "Building Facade and Envelope System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("fc_wall", "Wall Systems", 1, None),
    ("fc_glaze", "Glazing Systems", 1, None),
    ("fc_rain", "Rainscreen and Cladding", 1, None),
    ("fc_wall_cmu", "CMU / masonry wall", 2, "fc_wall"),
    ("fc_wall_sip", "Structural insulated panels (SIPs)", 2, "fc_wall"),
    ("fc_wall_tilt", "Tilt-up concrete panels", 2, "fc_wall"),
    ("fc_wall_icf", "Insulated concrete forms (ICF)", 2, "fc_wall"),
    ("fc_glaze_curtain", "Curtain wall (stick, unitized)", 2, "fc_glaze"),
    ("fc_glaze_store", "Storefront glazing", 2, "fc_glaze"),
    ("fc_glaze_window", "Window wall", 2, "fc_glaze"),
    ("fc_glaze_point", "Point-supported glazing", 2, "fc_glaze"),
    ("fc_glaze_double", "Double-skin facade", 2, "fc_glaze"),
    ("fc_rain_metal", "Metal panel rainscreen (ACM, zinc, copper)", 2, "fc_rain"),
    ("fc_rain_terra", "Terracotta rainscreen", 2, "fc_rain"),
    ("fc_rain_fiber", "Fiber cement panel", 2, "fc_rain"),
    ("fc_rain_stone", "Natural stone cladding", 2, "fc_rain"),
]


async def ingest_domain_facade(conn) -> int:
    """Insert or update Facade System Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_facade"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_facade", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_facade",
    )
    return count
