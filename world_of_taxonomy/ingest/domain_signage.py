"""Ingest Building and Site Signage Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_signage",
    "Signage Types",
    "Building and Site Signage Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sg_ext", "Exterior Signage", 1, None),
    ("sg_int", "Interior Signage", 1, None),
    ("sg_digital", "Digital Signage", 1, None),
    ("sg_ext_monument", "Monument signs (ground, pylon)", 2, "sg_ext"),
    ("sg_ext_channel", "Channel letters (front-lit, back-lit, halo)", 2, "sg_ext"),
    ("sg_ext_awning", "Awning and canopy signs", 2, "sg_ext"),
    ("sg_ext_banner", "Banners and flag signs", 2, "sg_ext"),
    ("sg_ext_wall", "Wall-mounted signs (flat, projecting)", 2, "sg_ext"),
    ("sg_int_wayfind", "Wayfinding and directional signs", 2, "sg_int"),
    ("sg_int_ada", "ADA-compliant signs (tactile, braille)", 2, "sg_int"),
    ("sg_int_lobby", "Lobby and tenant directory", 2, "sg_int"),
    ("sg_int_safety", "Safety and regulatory signs", 2, "sg_int"),
    ("sg_digital_led", "LED displays (outdoor, indoor)", 2, "sg_digital"),
    ("sg_digital_lcd", "LCD and video wall displays", 2, "sg_digital"),
    ("sg_digital_inter", "Interactive kiosks and touchscreens", 2, "sg_digital"),
    ("sg_digital_menu", "Digital menu boards", 2, "sg_digital"),
]


async def ingest_domain_signage(conn) -> int:
    """Insert or update Signage Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_signage"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_signage", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_signage",
    )
    return count
