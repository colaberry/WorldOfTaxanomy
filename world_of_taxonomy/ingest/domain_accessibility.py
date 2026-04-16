"""Ingest Building Accessibility and Universal Design Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_accessibility",
    "Accessibility Feature Types",
    "Building Accessibility and Universal Design Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ax_phys", "Physical Accessibility", 1, None),
    ("ax_sense", "Sensory Accessibility", 1, None),
    ("ax_tech", "Technology Accessibility", 1, None),
    ("ax_phys_ramp", "Ramps and accessible routes", 2, "ax_phys"),
    ("ax_phys_elev", "Accessible elevators and lifts", 2, "ax_phys"),
    ("ax_phys_restroom", "Accessible restrooms", 2, "ax_phys"),
    ("ax_phys_park", "Accessible parking (van, standard)", 2, "ax_phys"),
    ("ax_phys_door", "Automatic doors and hardware", 2, "ax_phys"),
    ("ax_sense_visual", "Visual aids (tactile paving, high-contrast)", 2, "ax_sense"),
    ("ax_sense_audio", "Audio aids (hearing loops, PA systems)", 2, "ax_sense"),
    ("ax_sense_wayfind", "Accessible wayfinding (tactile maps, audio beacons)", 2, "ax_sense"),
    ("ax_sense_braille", "Braille signage and labels", 2, "ax_sense"),
    ("ax_tech_assist", "Assistive listening systems", 2, "ax_tech"),
    ("ax_tech_caption", "Real-time captioning systems", 2, "ax_tech"),
    ("ax_tech_voice", "Voice-activated controls", 2, "ax_tech"),
    ("ax_tech_smart", "Smart home accessibility features", 2, "ax_tech"),
]


async def ingest_domain_accessibility(conn) -> int:
    """Insert or update Accessibility Feature Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_accessibility"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_accessibility", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_accessibility",
    )
    return count
