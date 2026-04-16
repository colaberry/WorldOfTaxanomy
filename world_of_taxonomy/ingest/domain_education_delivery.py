"""Education Delivery Format and Modality Types domain taxonomy ingester.

Classifies educational programs and institutions by their delivery format.
Orthogonal to program type, funding source, and learner segment.
Based on IPEDS instructional modality definitions, LearningHouse / Wiley
online learning taxonomy, and OECD education modality framework.
Used by edtech vendors, accreditors, workforce programs, and institutional planners.

Code prefix: dedudlv_
System ID: domain_education_delivery
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
EDUCATION_DELIVERY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dedudlv_inperson", "In-Person and Residential Delivery", 1, None),
    ("dedudlv_inperson_residential", "Residential campus: students live and learn on-site", 2, "dedudlv_inperson"),
    ("dedudlv_inperson_commuter", "Commuter campus: local day students, no on-campus residency", 2, "dedudlv_inperson"),
    ("dedudlv_inperson_lab", "Lab and clinical in-person components (medicine, engineering, art)", 2, "dedudlv_inperson"),
    ("dedudlv_online", "Online and Distance Education Delivery", 1, None),
    ("dedudlv_online_async", "Fully asynchronous online (self-paced, no live sessions)", 2, "dedudlv_online"),
    ("dedudlv_online_sync", "Synchronous online (live virtual classroom, scheduled sessions)", 2, "dedudlv_online"),
    ("dedudlv_online_mooc", "MOOC and open online courseware (Coursera, edX, FutureLearn)", 2, "dedudlv_online"),
    ("dedudlv_hybrid", "Hybrid and Blended Learning", 1, None),
    ("dedudlv_hybrid_blended", "Blended: combines online modules with in-person class meetings", 2, "dedudlv_hybrid"),
    ("dedudlv_hybrid_hyflex", "HyFlex: students choose in-person or online each session", 2, "dedudlv_hybrid"),
    ("dedudlv_intensive", "Intensive and Accelerated Formats", 1, None),
    ("dedudlv_intensive_bootcamp", "Bootcamp / immersive intensive (coding, data science, UX - 12-24 weeks)", 2, "dedudlv_intensive"),
    ("dedudlv_intensive_cohort", "Cohort-based learning with fixed start/end and peer cohort", 2, "dedudlv_intensive"),
    ("dedudlv_intensive_weekend", "Weekend / executive weekend format (MBA, professional programs)", 2, "dedudlv_intensive"),
    ("dedudlv_competency", "Competency-Based and Self-Paced Formats", 1, None),
    ("dedudlv_competency_cbe", "Competency-based education (CBE): advance by demonstrating mastery", 2, "dedudlv_competency"),
    ("dedudlv_competency_selfpaced", "Self-paced with mentor check-ins (WGU model, direct assessment)", 2, "dedudlv_competency"),
]

_DOMAIN_ROW = (
    "domain_education_delivery",
    "Education Delivery Format and Modality Types",
    "Education program delivery format and instructional modality classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['61', '6111', '6112', '6113']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_education_delivery(conn) -> int:
    """Ingest Education Delivery Format and Modality Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_education_delivery'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_education_delivery",
        "Education Delivery Format and Modality Types",
        "Education program delivery format and instructional modality classification",
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

    parent_codes = {parent for _, _, _, parent in EDUCATION_DELIVERY_NODES if parent is not None}

    rows = [
        (
            "domain_education_delivery",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in EDUCATION_DELIVERY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(EDUCATION_DELIVERY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_education_delivery'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_education_delivery'",
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
            [("naics_2022", code, "domain_education_delivery", "primary") for code in naics_codes],
        )

    return count
