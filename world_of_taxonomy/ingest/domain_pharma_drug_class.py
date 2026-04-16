"""Ingest Pharmaceutical Drug Classification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_pharma_drug_class",
    "Pharma Drug Classification",
    "Pharmaceutical Drug Classification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pdc_class", "Drug Classification Methods", 1, None),
    ("pdc_route", "Routes of Administration", 1, None),
    ("pdc_form", "Dosage Forms", 1, None),
    ("pdc_dev", "Drug Development Stages", 1, None),
    ("pdc_class_chem", "Chemical/Pharmacological classification", 2, "pdc_class"),
    ("pdc_class_ther", "Therapeutic classification", 2, "pdc_class"),
    ("pdc_class_moa", "Mechanism of action classification", 2, "pdc_class"),
    ("pdc_class_rx", "Prescription vs OTC classification", 2, "pdc_class"),
    ("pdc_class_bio", "Biologic vs small molecule", 2, "pdc_class"),
    ("pdc_route_oral", "Oral (tablets, capsules, liquids)", 2, "pdc_route"),
    ("pdc_route_iv", "Intravenous and infusion", 2, "pdc_route"),
    ("pdc_route_im", "Intramuscular injection", 2, "pdc_route"),
    ("pdc_route_sc", "Subcutaneous injection", 2, "pdc_route"),
    ("pdc_route_top", "Topical (creams, patches, gels)", 2, "pdc_route"),
    ("pdc_route_inh", "Inhalation (MDI, DPI, nebulizer)", 2, "pdc_route"),
    ("pdc_route_rec", "Rectal and vaginal", 2, "pdc_route"),
    ("pdc_form_solid", "Solid dosage forms (tablet, capsule, powder)", 2, "pdc_form"),
    ("pdc_form_liq", "Liquid dosage forms (solution, suspension, emulsion)", 2, "pdc_form"),
    ("pdc_form_semi", "Semi-solid forms (cream, ointment, gel)", 2, "pdc_form"),
    ("pdc_form_spec", "Specialty forms (implant, depot, liposomal)", 2, "pdc_form"),
    ("pdc_dev_disc", "Discovery and preclinical", 2, "pdc_dev"),
    ("pdc_dev_p1", "Phase I clinical trials", 2, "pdc_dev"),
    ("pdc_dev_p2", "Phase II clinical trials", 2, "pdc_dev"),
    ("pdc_dev_p3", "Phase III clinical trials", 2, "pdc_dev"),
    ("pdc_dev_reg", "Regulatory submission and approval", 2, "pdc_dev"),
    ("pdc_dev_p4", "Phase IV post-market surveillance", 2, "pdc_dev"),
]


async def ingest_domain_pharma_drug_class(conn) -> int:
    """Insert or update Pharma Drug Classification system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_pharma_drug_class"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_pharma_drug_class", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_pharma_drug_class",
    )
    return count
