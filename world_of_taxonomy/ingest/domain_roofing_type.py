"""Ingest Roofing System and Material Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_roofing_type",
    "Roofing Types",
    "Roofing System and Material Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("rf_steep", "Steep-Slope Roofing", 1, None),
    ("rf_low", "Low-Slope Roofing", 1, None),
    ("rf_green", "Sustainable Roofing", 1, None),
    ("rf_steep_asph", "Asphalt shingles (3-tab, architectural)", 2, "rf_steep"),
    ("rf_steep_metal", "Metal roofing (standing seam, corrugated)", 2, "rf_steep"),
    ("rf_steep_tile", "Tile roofing (clay, concrete)", 2, "rf_steep"),
    ("rf_steep_slate", "Slate roofing", 2, "rf_steep"),
    ("rf_steep_wood", "Wood shakes and shingles", 2, "rf_steep"),
    ("rf_low_tpo", "TPO membrane", 2, "rf_low"),
    ("rf_low_epdm", "EPDM rubber membrane", 2, "rf_low"),
    ("rf_low_pvc", "PVC membrane", 2, "rf_low"),
    ("rf_low_bur", "Built-up roof (BUR / tar and gravel)", 2, "rf_low"),
    ("rf_low_mod", "Modified bitumen", 2, "rf_low"),
    ("rf_green_veg", "Green / vegetated roof (extensive, intensive)", 2, "rf_green"),
    ("rf_green_cool", "Cool roofing (reflective coatings)", 2, "rf_green"),
    ("rf_green_solar", "Solar-ready / BIPV roofing", 2, "rf_green"),
]


async def ingest_domain_roofing_type(conn) -> int:
    """Insert or update Roofing Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_roofing_type"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_roofing_type", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_roofing_type",
    )
    return count
