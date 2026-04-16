"""CCPA/CPRA regulatory taxonomy ingester.

California Consumer Privacy Act of 2018 / California Privacy Rights Act of 2020.
Authority: California Legislature / California Privacy Protection Agency.
Source: https://leginfo.legislature.ca.gov/faces/codes_displayText.xhtml?division=3.&part=4.&lawCode=CIV&title=1.81.5

Data provenance: manual_transcription
License: Public Domain

Total: 34 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_ccpa"
_SYSTEM_NAME = "CCPA/CPRA"
_FULL_NAME = "California Consumer Privacy Act of 2018 / California Privacy Rights Act of 2020"
_VERSION = "2020"
_REGION = "United States"
_AUTHORITY = "California Legislature / California Privacy Protection Agency"
_SOURCE_URL = "https://leginfo.legislature.ca.gov/faces/codes_displayText.xhtml?division=3.&part=4.&lawCode=CIV&title=1.81.5"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_CCPA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("definitions", "Definitions and Scope", 1, None),
    ("consumer_rights", "Consumer Rights", 1, None),
    ("business_duties", "Business Obligations", 1, None),
    ("service_providers", "Service Provider and Contractor Requirements", 1, None),
    ("enforcement", "Enforcement and Penalties", 1, None),
    ("cppa", "California Privacy Protection Agency", 1, None),
    ("def_personal_info", "Personal Information (1798.140(v))", 2, "definitions"),
    ("def_consumer", "Consumer (1798.140(i))", 2, "definitions"),
    ("def_business", "Business (1798.140(d))", 2, "definitions"),
    ("def_sensitive_pi", "Sensitive Personal Information (1798.140(ae))", 2, "definitions"),
    ("def_sale", "Sale of Personal Information (1798.140(ad))", 2, "definitions"),
    ("def_share", "Share/Cross-Context Behavioral Advertising (1798.140(ah))", 2, "definitions"),
    ("right_know", "Right to Know (1798.100, 1798.110)", 2, "consumer_rights"),
    ("right_delete", "Right to Delete (1798.105)", 2, "consumer_rights"),
    ("right_correct", "Right to Correct (1798.106)", 2, "consumer_rights"),
    ("right_opt_out", "Right to Opt-Out of Sale/Sharing (1798.120)", 2, "consumer_rights"),
    ("right_limit", "Right to Limit Use of Sensitive PI (1798.121)", 2, "consumer_rights"),
    ("right_nondiscrim", "Right to Non-Discrimination (1798.125)", 2, "consumer_rights"),
    ("right_portability", "Right to Data Portability (1798.130)", 2, "consumer_rights"),
    ("duty_notice", "Notice at Collection (1798.100(b))", 2, "business_duties"),
    ("duty_privacy_policy", "Privacy Policy Requirements (1798.130(a)(5))", 2, "business_duties"),
    ("duty_respond", "Responding to Consumer Requests (1798.130)", 2, "business_duties"),
    ("duty_data_min", "Data Minimization (1798.100(c))", 2, "business_duties"),
    ("duty_purpose_limit", "Purpose Limitation (1798.100(c))", 2, "business_duties"),
    ("duty_security", "Reasonable Security Procedures (1798.150)", 2, "business_duties"),
    ("sp_contract", "Service Provider Contractual Requirements (1798.140(ag))", 2, "service_providers"),
    ("contractor_req", "Contractor Requirements (1798.140(j))", 2, "service_providers"),
    ("enf_ag", "Attorney General Enforcement (pre-CPRA)", 2, "enforcement"),
    ("enf_cppa", "CPPA Administrative Enforcement (1798.199.40-90)", 2, "enforcement"),
    ("enf_private_right", "Private Right of Action for Data Breaches (1798.150)", 2, "enforcement"),
    ("enf_penalties", "Civil Penalties - Up to $7,500 per Intentional Violation", 2, "enforcement"),
    ("cppa_board", "CPPA Board Structure and Appointments", 2, "cppa"),
    ("cppa_rulemaking", "CPPA Rulemaking Authority (1798.185)", 2, "cppa"),
    ("cppa_audits", "CPPA Audit Authority", 2, "cppa"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_ccpa(conn) -> int:
    """Ingest CCPA/CPRA regulatory taxonomy.

    Returns 34 nodes.
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
    for code, title, level, parent in REG_CCPA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CCPA_NODES:
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
        for code, title, level, parent in REG_CCPA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CCPA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
