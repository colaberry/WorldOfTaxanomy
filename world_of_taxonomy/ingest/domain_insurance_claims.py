"""Ingest Insurance Claims Process and Category Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_insurance_claims",
    "Insurance Claims Types",
    "Insurance Claims Process and Category Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ic_type", "Claim Types", 1, None),
    ("ic_proc", "Claims Process", 1, None),
    ("ic_tech", "Claims Technology", 1, None),
    ("ic_type_first", "First-party claims (policyholder)", 2, "ic_type"),
    ("ic_type_third", "Third-party claims (liability)", 2, "ic_type"),
    ("ic_type_sub", "Subrogation claims", 2, "ic_type"),
    ("ic_type_cat", "Catastrophe claims", 2, "ic_type"),
    ("ic_type_fraud", "Fraudulent claims", 2, "ic_type"),
    ("ic_proc_fnol", "First notice of loss (FNOL)", 2, "ic_proc"),
    ("ic_proc_invest", "Claims investigation", 2, "ic_proc"),
    ("ic_proc_adj", "Claims adjustment and evaluation", 2, "ic_proc"),
    ("ic_proc_settle", "Settlement and payment", 2, "ic_proc"),
    ("ic_proc_lit", "Claims litigation management", 2, "ic_proc"),
    ("ic_tech_str", "Straight-through processing", 2, "ic_tech"),
    ("ic_tech_ai", "AI-assisted claims triage", 2, "ic_tech"),
    ("ic_tech_photo", "Photo and video damage assessment", 2, "ic_tech"),
    ("ic_tech_telem", "Telematics-based claims (auto)", 2, "ic_tech"),
]


async def ingest_domain_insurance_claims(conn) -> int:
    """Insert or update Insurance Claims Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_insurance_claims"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_insurance_claims", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_insurance_claims",
    )
    return count
