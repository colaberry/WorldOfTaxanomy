"""Ingest Circular Economy Business Model Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_circular_econ",
    "Circular Economy Types",
    "Circular Economy Business Model Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ce_strat", "Circular Strategies", 1, None),
    ("ce_model", "Business Models", 1, None),
    ("ce_mat", "Material Flows", 1, None),
    ("ce_strat_reduce", "Reduce (design for longevity, dematerialization)", 2, "ce_strat"),
    ("ce_strat_reuse", "Reuse (second-hand, refurbishment)", 2, "ce_strat"),
    ("ce_strat_repair", "Repair and maintenance services", 2, "ce_strat"),
    ("ce_strat_remfg", "Remanufacture", 2, "ce_strat"),
    ("ce_strat_recycle", "Recycle (mechanical, chemical)", 2, "ce_strat"),
    ("ce_model_paas", "Product-as-a-Service (PaaS)", 2, "ce_model"),
    ("ce_model_share", "Sharing platforms", 2, "ce_model"),
    ("ce_model_take", "Take-back programs", 2, "ce_model"),
    ("ce_model_ind", "Industrial symbiosis", 2, "ce_model"),
    ("ce_model_c2c", "Cradle-to-cradle design", 2, "ce_model"),
    ("ce_mat_bio", "Biological cycle (compost, anaerobic digestion)", 2, "ce_mat"),
    ("ce_mat_tech", "Technical cycle (metals, plastics recovery)", 2, "ce_mat"),
    ("ce_mat_ewaste", "E-waste recycling", 2, "ce_mat"),
    ("ce_mat_textile", "Textile recycling", 2, "ce_mat"),
    ("ce_mat_constr", "Construction and demolition waste recovery", 2, "ce_mat"),
]


async def ingest_domain_circular_econ(conn) -> int:
    """Insert or update Circular Economy Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_circular_econ"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_circular_econ", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_circular_econ",
    )
    return count
