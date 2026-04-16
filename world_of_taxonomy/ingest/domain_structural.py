"""Ingest Building Structural System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_structural",
    "Structural System Types",
    "Building Structural System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("st_mat", "Structural Materials", 1, None),
    ("st_sys", "Structural Systems", 1, None),
    ("st_lat", "Lateral Force Systems", 1, None),
    ("st_mat_steel", "Structural steel", 2, "st_mat"),
    ("st_mat_conc", "Reinforced concrete", 2, "st_mat"),
    ("st_mat_pst", "Pre-stressed concrete", 2, "st_mat"),
    ("st_mat_timber", "Mass timber (CLT, glulam)", 2, "st_mat"),
    ("st_mat_mason", "Masonry (CMU, brick)", 2, "st_mat"),
    ("st_sys_frame", "Moment frame", 2, "st_sys"),
    ("st_sys_brace", "Braced frame (X, V, chevron)", 2, "st_sys"),
    ("st_sys_shear", "Shear wall system", 2, "st_sys"),
    ("st_sys_tube", "Tubular structure (framed, bundled)", 2, "st_sys"),
    ("st_sys_flat", "Flat plate / flat slab", 2, "st_sys"),
    ("st_lat_smrf", "Special moment resisting frame (SMRF)", 2, "st_lat"),
    ("st_lat_scbf", "Special concentrically braced frame", 2, "st_lat"),
    ("st_lat_ecbf", "Eccentrically braced frame", 2, "st_lat"),
    ("st_lat_brbf", "Buckling-restrained braced frame", 2, "st_lat"),
]


async def ingest_domain_structural(conn) -> int:
    """Insert or update Structural System Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_structural"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_structural", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_structural",
    )
    return count
