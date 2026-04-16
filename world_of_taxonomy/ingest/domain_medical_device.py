"""Ingest Medical Device Classification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_medical_device",
    "Medical Device Classification",
    "Medical Device Classification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("mdc_risk", "Risk Classification", 1, None),
    ("mdc_type", "Device Categories", 1, None),
    ("mdc_reg", "Regulatory Pathways", 1, None),
    ("mdc_risk_1", "Class I (low risk, general controls)", 2, "mdc_risk"),
    ("mdc_risk_2", "Class II (moderate risk, special controls)", 2, "mdc_risk"),
    ("mdc_risk_3", "Class III (high risk, premarket approval)", 2, "mdc_risk"),
    ("mdc_type_diag", "Diagnostic devices (in vitro, imaging)", 2, "mdc_type"),
    ("mdc_type_ther", "Therapeutic devices (implants, prosthetics)", 2, "mdc_type"),
    ("mdc_type_surg", "Surgical instruments and tools", 2, "mdc_type"),
    ("mdc_type_mon", "Patient monitoring devices", 2, "mdc_type"),
    ("mdc_type_life", "Life support and resuscitation", 2, "mdc_type"),
    ("mdc_type_soft", "Software as medical device (SaMD)", 2, "mdc_type"),
    ("mdc_type_comb", "Combination products (drug-device)", 2, "mdc_type"),
    ("mdc_type_ivd", "In vitro diagnostics (IVD)", 2, "mdc_type"),
    ("mdc_reg_510k", "510(k) premarket notification", 2, "mdc_reg"),
    ("mdc_reg_pma", "Premarket approval (PMA)", 2, "mdc_reg"),
    ("mdc_reg_denovo", "De Novo classification", 2, "mdc_reg"),
    ("mdc_reg_hde", "Humanitarian device exemption", 2, "mdc_reg"),
    ("mdc_reg_ce", "CE marking (EU MDR)", 2, "mdc_reg"),
]


async def ingest_domain_medical_device(conn) -> int:
    """Insert or update Medical Device Classification system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_medical_device"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_medical_device", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_medical_device",
    )
    return count
