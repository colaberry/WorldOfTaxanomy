"""Ingest Franchise Business Model and Structure Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_franchise",
    "Franchise Model Types",
    "Franchise Business Model and Structure Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("fran_type", "Franchise Types", 1, None),
    ("fran_sector", "Sector Categories", 1, None),
    ("fran_ops", "Operational Models", 1, None),
    ("fran_type_biz", "Business format franchise", 2, "fran_type"),
    ("fran_type_prod", "Product distribution franchise", 2, "fran_type"),
    ("fran_type_mfg", "Manufacturing franchise", 2, "fran_type"),
    ("fran_type_conv", "Conversion franchise", 2, "fran_type"),
    ("fran_sector_food", "Food and restaurant franchises", 2, "fran_sector"),
    ("fran_sector_retail", "Retail franchises", 2, "fran_sector"),
    ("fran_sector_svc", "Service franchises (cleaning, repair)", 2, "fran_sector"),
    ("fran_sector_health", "Health and fitness franchises", 2, "fran_sector"),
    ("fran_sector_edu", "Education and tutoring franchises", 2, "fran_sector"),
    ("fran_sector_auto", "Automotive service franchises", 2, "fran_sector"),
    ("fran_ops_single", "Single-unit franchisee", 2, "fran_ops"),
    ("fran_ops_multi", "Multi-unit franchisee", 2, "fran_ops"),
    ("fran_ops_area", "Area developer", 2, "fran_ops"),
    ("fran_ops_master", "Master franchisee (sub-franchisor)", 2, "fran_ops"),
]


async def ingest_domain_franchise(conn) -> int:
    """Insert or update Franchise Model Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_franchise"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_franchise", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_franchise",
    )
    return count
