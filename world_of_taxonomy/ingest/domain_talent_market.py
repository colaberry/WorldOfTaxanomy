"""Ingest Talent Marketplace and Workforce Platform Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_talent_market",
    "Talent Marketplace Types",
    "Talent Marketplace and Workforce Platform Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("tm_type", "Platform Types", 1, None),
    ("tm_worker", "Worker Categories", 1, None),
    ("tm_model", "Engagement Models", 1, None),
    ("tm_type_free", "Freelance marketplace (Upwork, Fiverr)", 2, "tm_type"),
    ("tm_type_staff", "Staffing platform (temp, contract)", 2, "tm_type"),
    ("tm_type_exec", "Executive search platform", 2, "tm_type"),
    ("tm_type_internal", "Internal talent marketplace", 2, "tm_type"),
    ("tm_type_direct", "Direct sourcing platform", 2, "tm_type"),
    ("tm_worker_know", "Knowledge workers (tech, consulting)", 2, "tm_worker"),
    ("tm_worker_creative", "Creative professionals (design, content)", 2, "tm_worker"),
    ("tm_worker_blue", "Blue-collar and skilled trades", 2, "tm_worker"),
    ("tm_worker_care", "Care workers (healthcare, childcare)", 2, "tm_worker"),
    ("tm_worker_gig", "Gig economy workers (delivery, rideshare)", 2, "tm_worker"),
    ("tm_model_proj", "Project-based engagement", 2, "tm_model"),
    ("tm_model_retain", "Retainer-based engagement", 2, "tm_model"),
    ("tm_model_fte", "Contract-to-hire (temp-to-perm)", 2, "tm_model"),
    ("tm_model_managed", "Managed services", 2, "tm_model"),
    ("tm_model_sow", "Statement of work (SOW)", 2, "tm_model"),
]


async def ingest_domain_talent_market(conn) -> int:
    """Insert or update Talent Marketplace Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_talent_market"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_talent_market", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_talent_market",
    )
    return count
