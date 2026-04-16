"""COPPA regulatory taxonomy ingester.

Children's Online Privacy Protection Act of 1998 (15 USC 6501-6506).
Authority: US Congress / FTC.
Source: https://www.ecfr.gov/current/title-16/chapter-I/subchapter-C/part-312

Data provenance: manual_transcription
License: Public Domain

Total: 23 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_coppa"
_SYSTEM_NAME = "COPPA"
_FULL_NAME = "Children's Online Privacy Protection Act of 1998 (15 USC 6501-6506)"
_VERSION = "1998"
_REGION = "United States"
_AUTHORITY = "US Congress / FTC"
_SOURCE_URL = "https://www.ecfr.gov/current/title-16/chapter-I/subchapter-C/part-312"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_COPPA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("notice", "Notice Requirements", 1, None),
    ("consent", "Parental Consent", 1, None),
    ("data_practices", "Data Collection and Use", 1, None),
    ("operator_duties", "Operator Obligations", 1, None),
    ("safe_harbor", "FTC Safe Harbor Programs", 1, None),
    ("enforcement", "Enforcement and Penalties", 1, None),
    ("notice_online", "Online Notice Requirements (312.4(a))", 2, "notice"),
    ("notice_direct", "Direct Notice to Parents (312.4(b))", 2, "notice"),
    ("notice_content", "Content of Notice (312.4(c))", 2, "notice"),
    ("consent_verifiable", "Verifiable Parental Consent Methods (312.5)", 2, "consent"),
    ("consent_exceptions", "Exceptions to Prior Consent (312.5(c))", 2, "consent"),
    ("consent_revocation", "Right to Revoke Consent (312.6)", 2, "consent"),
    ("data_collection", "Limitations on Collection (312.7)", 2, "data_practices"),
    ("data_retention", "Data Retention and Deletion (312.10)", 2, "data_practices"),
    ("data_security", "Confidentiality and Security (312.8)", 2, "data_practices"),
    ("op_website", "Website and Online Service Obligations", 2, "operator_duties"),
    ("op_third_party", "Third-Party Collection Oversight (312.2)", 2, "operator_duties"),
    ("op_schools", "School Authorization (312.5(b)(2))", 2, "operator_duties"),
    ("sh_programs", "Approved Safe Harbor Programs (312.11)", 2, "safe_harbor"),
    ("sh_requirements", "Self-Regulatory Program Requirements (312.11(b))", 2, "safe_harbor"),
    ("enf_ftc", "FTC Enforcement Authority (312.9)", 2, "enforcement"),
    ("enf_civil", "Civil Penalties (312.9)", 2, "enforcement"),
    ("enf_state_ag", "State Attorney General Actions", 2, "enforcement"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_coppa(conn) -> int:
    """Ingest COPPA regulatory taxonomy.

    Returns 23 nodes.
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
    for code, title, level, parent in REG_COPPA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_COPPA_NODES:
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
        for code, title, level, parent in REG_COPPA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_COPPA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
