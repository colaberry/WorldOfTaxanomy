"""Ingest Sustainable and Green Building Material Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_green_material",
    "Green Building Material Types",
    "Sustainable and Green Building Material Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("gm_struct", "Structural Materials", 1, None),
    ("gm_finish", "Finish Materials", 1, None),
    ("gm_insul", "Insulation Materials", 1, None),
    ("gm_struct_timber", "Mass timber (CLT, NLT, DLT, glulam)", 2, "gm_struct"),
    ("gm_struct_recyc", "Recycled steel and reclaimed materials", 2, "gm_struct"),
    ("gm_struct_geo", "Geopolymer concrete (low-carbon)", 2, "gm_struct"),
    ("gm_struct_bamboo", "Bamboo structural elements", 2, "gm_struct"),
    ("gm_struct_hemp", "Hempcrete", 2, "gm_struct"),
    ("gm_finish_recl", "Reclaimed wood flooring and paneling", 2, "gm_finish"),
    ("gm_finish_cork", "Cork flooring", 2, "gm_finish"),
    ("gm_finish_linol", "Linoleum (natural)", 2, "gm_finish"),
    ("gm_finish_low", "Low-VOC paints and adhesives", 2, "gm_finish"),
    ("gm_finish_recyc", "Recycled glass and ceramic tile", 2, "gm_finish"),
    ("gm_insul_cellulose", "Cellulose insulation", 2, "gm_insul"),
    ("gm_insul_wool", "Mineral wool (rock, slag)", 2, "gm_insul"),
    ("gm_insul_sheep", "Sheep wool insulation", 2, "gm_insul"),
    ("gm_insul_aero", "Aerogel insulation", 2, "gm_insul"),
]


async def ingest_domain_green_material(conn) -> int:
    """Insert or update Green Building Material Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_green_material"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_green_material", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_green_material",
    )
    return count
