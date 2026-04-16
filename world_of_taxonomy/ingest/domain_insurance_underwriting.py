"""Ingest Insurance Underwriting Process and Method Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_insurance_underwriting",
    "Insurance Underwriting Types",
    "Insurance Underwriting Process and Method Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("iu_life", "Life Underwriting", 1, None),
    ("iu_pc", "Property and Casualty Underwriting", 1, None),
    ("iu_meth", "Underwriting Methods", 1, None),
    ("iu_life_med", "Medical underwriting (paramedical, APS)", 2, "iu_life"),
    ("iu_life_fin", "Financial underwriting", 2, "iu_life"),
    ("iu_life_accel", "Accelerated underwriting (algorithmic)", 2, "iu_life"),
    ("iu_life_simpl", "Simplified issue (guaranteed, simplified)", 2, "iu_life"),
    ("iu_pc_comm", "Commercial lines underwriting", 2, "iu_pc"),
    ("iu_pc_pers", "Personal lines underwriting", 2, "iu_pc"),
    ("iu_pc_spec", "Specialty and surplus lines underwriting", 2, "iu_pc"),
    ("iu_pc_cat", "Catastrophe underwriting", 2, "iu_pc"),
    ("iu_meth_manual", "Manual underwriting", 2, "iu_meth"),
    ("iu_meth_auto", "Automated/algorithmic underwriting", 2, "iu_meth"),
    ("iu_meth_pred", "Predictive model-based underwriting", 2, "iu_meth"),
    ("iu_meth_port", "Portfolio underwriting", 2, "iu_meth"),
]


async def ingest_domain_insurance_underwriting(conn) -> int:
    """Insert or update Insurance Underwriting Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_insurance_underwriting"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_insurance_underwriting", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_insurance_underwriting",
    )
    return count
