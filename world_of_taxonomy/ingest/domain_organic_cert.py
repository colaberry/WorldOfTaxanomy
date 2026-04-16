"""Ingest Organic and Sustainable Certification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_organic_cert",
    "Organic Certification Types",
    "Organic and Sustainable Certification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("oc_food", "Food and Agriculture Certifications", 1, None),
    ("oc_prod", "Product Certifications", 1, None),
    ("oc_sustain", "Sustainability Certifications", 1, None),
    ("oc_food_usda", "USDA Organic", 2, "oc_food"),
    ("oc_food_eu", "EU Organic Regulation", 2, "oc_food"),
    ("oc_food_dem", "Demeter (biodynamic)", 2, "oc_food"),
    ("oc_food_rgn", "Regenerative Organic Certified (ROC)", 2, "oc_food"),
    ("oc_food_non", "Non-GMO Project Verified", 2, "oc_food"),
    ("oc_prod_gots", "GOTS (Global Organic Textile Standard)", 2, "oc_prod"),
    ("oc_prod_cos", "COSMOS Organic (cosmetics)", 2, "oc_prod"),
    ("oc_prod_eco", "Ecocert", 2, "oc_prod"),
    ("oc_sustain_fair", "Fairtrade International", 2, "oc_sustain"),
    ("oc_sustain_rain", "Rainforest Alliance", 2, "oc_sustain"),
    ("oc_sustain_msc", "MSC (Marine Stewardship Council)", 2, "oc_sustain"),
    ("oc_sustain_fsc", "FSC (Forest Stewardship Council)", 2, "oc_sustain"),
    ("oc_sustain_bcorp", "B Corp Certification", 2, "oc_sustain"),
]


async def ingest_domain_organic_cert(conn) -> int:
    """Insert or update Organic Certification Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_organic_cert"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_organic_cert", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_organic_cert",
    )
    return count
