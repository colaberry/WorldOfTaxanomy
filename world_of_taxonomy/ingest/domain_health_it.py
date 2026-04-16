"""Health Information Technology System Types domain taxonomy ingester.

Classifies health information technology systems and digital health platforms.
Orthogonal to care setting, delivery model, and payer type.
Aligned with ONC Health IT certification criteria, HIMSS EMRAM stages,
and KLAS Research HIT product taxonomy.
Used by HIT buyers, vendors, consultants and investors evaluating the
health IT market landscape.

Code prefix: dhlcit_
System ID: domain_health_it
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
HEALTH_IT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dhlcit_ehr", "Electronic Health Record (EHR) and Clinical Systems", 1, None),
    ("dhlcit_ehr_ambulatory", "Ambulatory EHR (Epic Ambulatory, Athenahealth, eClinicalWorks)", 2, "dhlcit_ehr"),
    ("dhlcit_ehr_inpatient", "Inpatient EHR / CPOE (Epic Inpatient, Cerner PowerChart)", 2, "dhlcit_ehr"),
    ("dhlcit_ehr_ltpac", "Long-term and post-acute care EHR (PointClickCare, MatrixCare)", 2, "dhlcit_ehr"),
    ("dhlcit_rev", "Revenue Cycle Management (RCM) and Practice Management", 1, None),
    ("dhlcit_rev_rcm", "End-to-end RCM (charge capture, coding, billing, AR follow-up)", 2, "dhlcit_rev"),
    ("dhlcit_rev_pm", "Practice management systems (scheduling, eligibility, patient intake)", 2, "dhlcit_rev"),
    ("dhlcit_rev_coding", "Computer-assisted coding and clinical documentation improvement (CDI)", 2, "dhlcit_rev"),
    ("dhlcit_cdss", "Clinical Decision Support Systems (CDSS)", 1, None),
    ("dhlcit_cdss_alerts", "Alert and reminder systems (drug-drug interaction, preventive care)", 2, "dhlcit_cdss"),
    ("dhlcit_cdss_ai", "AI-powered CDSS (sepsis prediction, deterioration alerts, diagnostic AI)", 2, "dhlcit_cdss"),
    ("dhlcit_cdss_evidence", "Evidence-based order sets and care pathway automation", 2, "dhlcit_cdss"),
    ("dhlcit_population", "Population Health Management Platforms", 1, None),
    ("dhlcit_population_analytics", "Claims and clinical analytics for risk stratification", 2, "dhlcit_population"),
    ("dhlcit_population_care", "Care management and chronic disease management platforms", 2, "dhlcit_population"),
    ("dhlcit_engagement", "Patient Engagement and Consumer Health Platforms", 1, None),
    ("dhlcit_engagement_portal", "Patient portal and patient-generated data (MyChart, FollowMyHealth)", 2, "dhlcit_engagement"),
    ("dhlcit_engagement_rpm", "Remote patient monitoring (RPM) and virtual care platforms", 2, "dhlcit_engagement"),
    ("dhlcit_imaging", "Medical Imaging and PACS Systems", 1, None),
    ("dhlcit_imaging_pacs", "Picture Archiving and Communication System (PACS/VNA)", 2, "dhlcit_imaging"),
    ("dhlcit_imaging_ai", "AI radiology and pathology image analysis (Aidoc, Viz.ai, Paige.ai)", 2, "dhlcit_imaging"),
]

_DOMAIN_ROW = (
    "domain_health_it",
    "Health Information Technology System Types",
    "Health IT system and digital health platform classification: EHR, HIT, CDSS, population health, patient engagement",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['6211', '6221', '5415']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_health_it(conn) -> int:
    """Ingest Health Information Technology System Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_health_it'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_health_it",
        "Health Information Technology System Types",
        "Health IT system and digital health platform classification: EHR, HIT, CDSS, population health, patient engagement",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in HEALTH_IT_NODES if parent is not None}

    rows = [
        (
            "domain_health_it",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in HEALTH_IT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(HEALTH_IT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_health_it'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_health_it'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_health_it", "primary") for code in naics_codes],
        )

    return count
