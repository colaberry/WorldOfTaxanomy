"""Ingest Landscape Architecture and Design Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_landscape",
    "Landscape Types",
    "Landscape Architecture and Design Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ls_hard", "Hardscape Elements", 1, None),
    ("ls_soft", "Softscape Elements", 1, None),
    ("ls_water", "Water Features and Management", 1, None),
    ("ls_hard_pave", "Paving (concrete, asphalt, permeable pavers)", 2, "ls_hard"),
    ("ls_hard_wall", "Retaining walls and seat walls", 2, "ls_hard"),
    ("ls_hard_deck", "Decking and boardwalks", 2, "ls_hard"),
    ("ls_hard_struct", "Pergolas, arbors, and shade structures", 2, "ls_hard"),
    ("ls_hard_play", "Playground and recreation features", 2, "ls_hard"),
    ("ls_soft_turf", "Turfgrass and lawns", 2, "ls_soft"),
    ("ls_soft_tree", "Trees (shade, ornamental, evergreen)", 2, "ls_soft"),
    ("ls_soft_shrub", "Shrubs and hedges", 2, "ls_soft"),
    ("ls_soft_peren", "Perennials and groundcovers", 2, "ls_soft"),
    ("ls_soft_native", "Native and pollinator gardens", 2, "ls_soft"),
    ("ls_water_irrig", "Irrigation systems (drip, spray, smart)", 2, "ls_water"),
    ("ls_water_rain", "Rain gardens and bioswales", 2, "ls_water"),
    ("ls_water_pond", "Ponds and water features", 2, "ls_water"),
    ("ls_water_storm", "Stormwater management (detention, retention)", 2, "ls_water"),
]


async def ingest_domain_landscape(conn) -> int:
    """Insert or update Landscape Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_landscape"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_landscape", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_landscape",
    )
    return count
