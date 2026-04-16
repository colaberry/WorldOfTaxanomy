"""AICPA Professional Standards regulatory taxonomy ingester.

American Institute of Certified Public Accountants Professional Standards.
Authority: American Institute of CPAs (AICPA).
Source: https://www.aicpa-cima.com/resources/landing/aicpa-professional-standards

Data provenance: manual_transcription
License: Proprietary (AICPA)

Total: 21 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_aicpa"
_SYSTEM_NAME = "AICPA Professional Standards"
_FULL_NAME = "American Institute of Certified Public Accountants Professional Standards"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "American Institute of CPAs (AICPA)"
_SOURCE_URL = "https://www.aicpa-cima.com/resources/landing/aicpa-professional-standards"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (AICPA)"

# (code, title, level, parent_code)
REG_AICPA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("audit_attest", "Statements on Auditing Standards (SAS)", 1, None),
    ("attestation", "Statements on Standards for Attestation Engagements (SSAE)", 1, None),
    ("accounting", "Statements on Standards for Accounting and Review (SSARS)", 1, None),
    ("consulting", "Statements on Standards for Consulting Services (SSCS)", 1, None),
    ("quality", "Quality Management Standards", 1, None),
    ("ethics", "Code of Professional Conduct", 1, None),
    ("sas_145", "SAS 145 - Understanding the Entity and Risk Assessment", 2, "audit_attest"),
    ("sas_146", "SAS 146 - Quality Management for an Engagement", 2, "audit_attest"),
    ("sas_134", "SAS 134 - Auditor Reporting and Amendments", 2, "audit_attest"),
    ("ssae_18", "SSAE 18 - Attestation Standards: Clarification and Recodification", 2, "attestation"),
    ("at_c_320", "AT-C 320 - Reporting on an Examination of Controls (SOC 1)", 2, "attestation"),
    ("at_c_205", "AT-C 205 - Examination Engagements", 2, "attestation"),
    ("ssars_25", "SSARS 25 - Materiality in a Review", 2, "accounting"),
    ("ar_c_80", "AR-C 80 - Compilation Engagements", 2, "accounting"),
    ("ar_c_90", "AR-C 90 - Review of Financial Statements", 2, "accounting"),
    ("sqms_1", "SQMS 1 - A Firm's System of Quality Management", 2, "quality"),
    ("sqms_2", "SQMS 2 - Engagement Quality Reviews", 2, "quality"),
    ("et_1", "ET Section 1 - Conceptual Framework for Members", 2, "ethics"),
    ("et_independence", "Independence Rule (ET 1.200)", 2, "ethics"),
    ("et_integrity", "Integrity and Objectivity (ET 1.100)", 2, "ethics"),
    ("et_confidentiality", "Confidentiality (ET 1.700)", 2, "ethics"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_aicpa(conn) -> int:
    """Ingest AICPA Professional Standards regulatory taxonomy.

    Returns 21 nodes.
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
    for code, title, level, parent in REG_AICPA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_AICPA_NODES:
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
        for code, title, level, parent in REG_AICPA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_AICPA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
