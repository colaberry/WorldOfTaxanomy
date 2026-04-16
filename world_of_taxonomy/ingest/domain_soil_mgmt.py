"""Ingest Soil Management Practice and Classification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_soil_mgmt",
    "Soil Management Types",
    "Soil Management Practice and Classification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sm_class", "Soil Classification", 1, None),
    ("sm_pract", "Management Practices", 1, None),
    ("sm_test", "Soil Testing", 1, None),
    ("sm_class_order", "USDA Soil Taxonomy orders", 2, "sm_class"),
    ("sm_class_text", "Texture classification (sand, silt, clay)", 2, "sm_class"),
    ("sm_class_ph", "pH classification (acidic, neutral, alkaline)", 2, "sm_class"),
    ("sm_class_drain", "Drainage classification", 2, "sm_class"),
    ("sm_pract_till", "Tillage practices (conventional, reduced, no-till)", 2, "sm_pract"),
    ("sm_pract_cover", "Cover cropping", 2, "sm_pract"),
    ("sm_pract_compost", "Composting and organic amendments", 2, "sm_pract"),
    ("sm_pract_biochar", "Biochar application", 2, "sm_pract"),
    ("sm_pract_terrace", "Terracing and contour farming", 2, "sm_pract"),
    ("sm_pract_drain", "Drainage and irrigation management", 2, "sm_pract"),
    ("sm_test_nutri", "Nutrient analysis (N, P, K, micronutrients)", 2, "sm_test"),
    ("sm_test_om", "Organic matter testing", 2, "sm_test"),
    ("sm_test_bio", "Soil biology and microbial analysis", 2, "sm_test"),
    ("sm_test_cec", "Cation exchange capacity (CEC)", 2, "sm_test"),
]


async def ingest_domain_soil_mgmt(conn) -> int:
    """Insert or update Soil Management Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_soil_mgmt"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_soil_mgmt", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_soil_mgmt",
    )
    return count
