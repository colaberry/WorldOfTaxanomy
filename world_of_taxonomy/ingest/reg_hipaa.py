"""HIPAA regulatory taxonomy ingester.

Health Insurance Portability and Accountability Act of 1996.
Authority: US Congress / HHS.
Source: https://www.govinfo.gov/content/pkg/PLAW-104publ191/html/PLAW-104publ191.htm

Data provenance: manual_transcription
License: Public Domain

Total: 36 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_hipaa"
_SYSTEM_NAME = "HIPAA"
_FULL_NAME = "Health Insurance Portability and Accountability Act of 1996"
_VERSION = "1996"
_REGION = "United States"
_AUTHORITY = "US Congress / HHS"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/PLAW-104publ191/html/PLAW-104publ191.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_HIPAA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Health Care Access, Portability, and Renewability", 1, None),
    ("title_2", "Title II - Preventing Health Care Fraud and Abuse; Administrative Simplification", 1, None),
    ("title_3", "Title III - Tax-Related Health Provisions", 1, None),
    ("title_4", "Title IV - Application and Enforcement of Group Health Plan Requirements", 1, None),
    ("title_5", "Title V - Revenue Offsets", 1, None),
    ("sec_101", "Sec 101 - Increased Portability Through Limitation on Preexisting Condition Exclusions", 2, "title_1"),
    ("sec_102", "Sec 102 - Prohibiting Discrimination Based on Health Status", 2, "title_1"),
    ("sec_103", "Sec 103 - Guaranteed Renewability in Multi-Employer Plans", 2, "title_1"),
    ("sec_104", "Sec 104 - Clarification of Certain Continuation Coverage Requirements", 2, "title_1"),
    ("sec_201", "Sec 201 - Fraud and Abuse Control Program", 2, "title_2"),
    ("sec_211", "Sec 211 - Medicare Integrity Program", 2, "title_2"),
    ("sec_221", "Sec 221 - Administrative Simplification", 2, "title_2"),
    ("sec_261", "Sec 261 - Purpose of Administrative Simplification", 2, "title_2"),
    ("sec_262", "Sec 262 - Code Sets and Transaction Standards", 2, "title_2"),
    ("sec_263", "Sec 263 - Privacy of Individually Identifiable Health Information", 2, "title_2"),
    ("sec_264", "Sec 264 - Recommendations for Privacy Standards (Privacy Rule)", 2, "title_2"),
    ("rule_privacy", "HIPAA Privacy Rule (45 CFR Part 160 and Subparts A and E of Part 164)", 3, "sec_264"),
    ("rule_security", "HIPAA Security Rule (45 CFR Part 160 and Subparts A and C of Part 164)", 3, "sec_262"),
    ("rule_breach", "HIPAA Breach Notification Rule (45 CFR Part 164 Subpart D)", 3, "sec_263"),
    ("rule_enforcement", "HIPAA Enforcement Rule (45 CFR Part 160 Subparts C, D, E)", 3, "sec_201"),
    ("rule_omnibus", "HIPAA Omnibus Rule (2013 Final Rule)", 3, "sec_264"),
    ("rule_transactions", "HIPAA Transaction Standards (45 CFR Part 162)", 3, "sec_262"),
    ("sec_301", "Sec 301 - Medical Savings Accounts", 2, "title_3"),
    ("sec_311", "Sec 311 - COBRA Clarifications", 2, "title_3"),
    ("sec_401", "Sec 401 - ERISA Amendments for Group Health Plans", 2, "title_4"),
    ("sec_421", "Sec 421 - Enforcement and Penalties", 2, "title_4"),
    ("sec_501", "Sec 501 - Individual Health Insurance Tax Deduction", 2, "title_5"),
    ("sec_511", "Sec 511 - Revenue Offset Provisions", 2, "title_5"),
    ("priv_use", "Use and Disclosure of PHI (164.502-164.514)", 3, "rule_privacy"),
    ("priv_rights", "Individual Rights (164.520-164.528)", 3, "rule_privacy"),
    ("priv_admin", "Administrative Requirements (164.530)", 3, "rule_privacy"),
    ("sec_admin_safeguards", "Administrative Safeguards (164.308)", 3, "rule_security"),
    ("sec_physical_safeguards", "Physical Safeguards (164.310)", 3, "rule_security"),
    ("sec_technical_safeguards", "Technical Safeguards (164.312)", 3, "rule_security"),
    ("sec_org_requirements", "Organizational Requirements (164.314)", 3, "rule_security"),
    ("sec_policies", "Policies and Procedures and Documentation (164.316)", 3, "rule_security"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_hipaa(conn) -> int:
    """Ingest HIPAA regulatory taxonomy.

    Returns 36 nodes.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0,
                   source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance,
                   license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )

    leaf_codes = set()
    parent_codes = set()
    for code, title, level, parent in REG_HIPAA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_HIPAA_NODES:
        if code not in parent_codes:
            leaf_codes.add(code)

    rows = [
        (
            _SYSTEM_ID,
            code,
            title,
            level,
            parent,
            code.split("_")[0],
            code in leaf_codes,
        )
        for code, title, level, parent in REG_HIPAA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_HIPAA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
