"""Education Credential and Award Types domain taxonomy ingester.

Classifies educational qualifications by credential type and level.
Orthogonal to delivery format, program type, and funding source.
Based on IPEDS CIP award level codes, ISCED 2011 qualification levels,
and Credential Engine Credential Type vocabulary.
Used by accreditors, workforce developers, employers and EdTech platforms
to map learning outcomes to labor market outcomes.

Code prefix: dedured_
System ID: domain_education_credential
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
EDUCATION_CREDENTIAL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dedured_cert", "Certificate and Short Credentials", 1, None),
    ("dedured_cert_nondeg", "Non-degree certificate (postsecondary, under 1 year)", 2, "dedured_cert"),
    ("dedured_cert_industrycert", "Industry certification (CompTIA, AWS, PMP, CPA - employer-valued)", 2, "dedured_cert"),
    ("dedured_cert_digital", "Digital badge and micro-credential (Credly, Badgr, LI Learning)", 2, "dedured_cert"),
    ("dedured_assoc", "Associate Degree (2-Year) Credentials", 1, None),
    ("dedured_assoc_aa", "Associate of Arts (AA): transfer-focused liberal arts", 2, "dedured_assoc"),
    ("dedured_assoc_as", "Associate of Science (AS): STEM and pre-professional", 2, "dedured_assoc"),
    ("dedured_assoc_aas", "Associate of Applied Science (AAS): workforce-terminal degree", 2, "dedured_assoc"),
    ("dedured_bachelor", "Bachelor's Degree (4-Year) Credentials", 1, None),
    ("dedured_bachelor_ba", "Bachelor of Arts (BA): liberal arts and humanities", 2, "dedured_bachelor"),
    ("dedured_bachelor_bs", "Bachelor of Science (BS): technical and professional fields", 2, "dedured_bachelor"),
    ("dedured_bachelor_bfa", "Bachelor of Fine Arts (BFA) and other specialized bachelor's", 2, "dedured_bachelor"),
    ("dedured_graduate", "Graduate and Professional Degrees", 1, None),
    ("dedured_graduate_masters", "Master's degree (MA, MS, MBA, MEd, MFA, MPH)", 2, "dedured_graduate"),
    ("dedured_graduate_professional", "First professional degree (JD, MD, PharmD, DDS, DVM)", 2, "dedured_graduate"),
    ("dedured_graduate_doctorate", "Doctorate (PhD, EdD, DNP, DBA - research and professional)", 2, "dedured_graduate"),
    ("dedured_nondeg", "Non-Degree and Continuing Education", 1, None),
    ("dedured_nondeg_ce", "Continuing education unit (CEU) and professional development", 2, "dedured_nondeg"),
    ("dedured_nondeg_audit", "Course audit with no credential (lifelong learning, edX audit)", 2, "dedured_nondeg"),
]

_DOMAIN_ROW = (
    "domain_education_credential",
    "Education Credential and Award Types",
    "Educational credential, degree type and industry certification classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['61', '6111', '6112', '6113', '6114']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_education_credential(conn) -> int:
    """Ingest Education Credential and Award Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_education_credential'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_education_credential",
        "Education Credential and Award Types",
        "Educational credential, degree type and industry certification classification",
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

    parent_codes = {parent for _, _, _, parent in EDUCATION_CREDENTIAL_NODES if parent is not None}

    rows = [
        (
            "domain_education_credential",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in EDUCATION_CREDENTIAL_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(EDUCATION_CREDENTIAL_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_education_credential'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_education_credential'",
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
            [("naics_2022", code, "domain_education_credential", "primary") for code in naics_codes],
        )

    return count
