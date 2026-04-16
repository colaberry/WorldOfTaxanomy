"""Ingest Commercial Lending Product and Structure Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_commercial_lending",
    "Commercial Lending Types",
    "Commercial Lending Product and Structure Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_term", "Term Loans", 1, None),
    ("cl_revolve", "Revolving Facilities", 1, None),
    ("cl_spec", "Specialized Lending", 1, None),
    ("cl_term_senior", "Senior secured term loan", 2, "cl_term"),
    ("cl_term_sub", "Subordinated / mezzanine debt", 2, "cl_term"),
    ("cl_term_uni", "Unitranche facility", 2, "cl_term"),
    ("cl_term_delay", "Delayed draw term loan", 2, "cl_term"),
    ("cl_revolve_abl", "Asset-based lending (ABL) revolver", 2, "cl_revolve"),
    ("cl_revolve_cf", "Cash flow revolver", 2, "cl_revolve"),
    ("cl_revolve_swingline", "Swingline facility", 2, "cl_revolve"),
    ("cl_spec_cre", "Commercial real estate (CRE) lending", 2, "cl_spec"),
    ("cl_spec_constr", "Construction lending", 2, "cl_spec"),
    ("cl_spec_lever", "Leveraged lending", 2, "cl_spec"),
    ("cl_spec_syndic", "Syndicated loan", 2, "cl_spec"),
    ("cl_spec_club", "Club deal", 2, "cl_spec"),
    ("cl_spec_direct", "Direct lending (private credit)", 2, "cl_spec"),
]


async def ingest_domain_commercial_lending(conn) -> int:
    """Insert or update Commercial Lending Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_commercial_lending"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_commercial_lending", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_commercial_lending",
    )
    return count
