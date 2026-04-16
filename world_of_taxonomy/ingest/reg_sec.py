"""SEC Regulations regulatory taxonomy ingester.

Securities and Exchange Commission Key Regulations (17 CFR).
Authority: US Securities and Exchange Commission (SEC).
Source: https://www.ecfr.gov/current/title-17

Data provenance: manual_transcription
License: Public Domain

Total: 29 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_sec"
_SYSTEM_NAME = "SEC Regulations"
_FULL_NAME = "Securities and Exchange Commission Key Regulations (17 CFR)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Securities and Exchange Commission (SEC)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-17"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_SEC_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("disclosure", "Disclosure Regulations", 1, None),
    ("trading", "Trading and Market Regulations", 1, None),
    ("registration", "Registration and Offering Regulations", 1, None),
    ("governance", "Corporate Governance Regulations", 1, None),
    ("enforcement_sec", "Enforcement Framework", 1, None),
    ("reg_sk", "Regulation S-K - Non-Financial Statement Disclosures (17 CFR 229)", 2, "disclosure"),
    ("reg_sx", "Regulation S-X - Financial Statement Requirements (17 CFR 210)", 2, "disclosure"),
    ("reg_fd", "Regulation FD - Fair Disclosure (17 CFR 243)", 2, "disclosure"),
    ("reg_g", "Regulation G - Non-GAAP Financial Measures (17 CFR 244)", 2, "disclosure"),
    ("form_10k", "Form 10-K Annual Report Requirements", 2, "disclosure"),
    ("form_10q", "Form 10-Q Quarterly Report Requirements", 2, "disclosure"),
    ("form_8k", "Form 8-K Current Report Requirements", 2, "disclosure"),
    ("xbrl", "Inline XBRL Requirements", 2, "disclosure"),
    ("reg_nms", "Regulation NMS - National Market System (17 CFR 242)", 2, "trading"),
    ("reg_sho", "Regulation SHO - Short Sales (17 CFR 242.200-204)", 2, "trading"),
    ("reg_ats", "Regulation ATS - Alternative Trading Systems (17 CFR 242.300-303)", 2, "trading"),
    ("reg_sci", "Regulation SCI - Systems Compliance and Integrity", 2, "trading"),
    ("rule_10b5", "Rule 10b-5 - Employment of Manipulative and Deceptive Devices", 2, "trading"),
    ("reg_s", "Regulation S - Offshore Offerings (17 CFR 230.901-905)", 2, "registration"),
    ("reg_d", "Regulation D - Private Placements (17 CFR 230.500-508)", 2, "registration"),
    ("reg_a", "Regulation A - Small Offerings (17 CFR 230.251-263)", 2, "registration"),
    ("reg_cf", "Regulation Crowdfunding (17 CFR 227)", 2, "registration"),
    ("proxy_rules", "Proxy Rules (14a Series)", 2, "governance"),
    ("beneficial_own", "Beneficial Ownership Reporting (13d, 13f, 13g)", 2, "governance"),
    ("insider_trading", "Insider Trading Rules (Section 16, Rule 10b5-1)", 2, "governance"),
    ("whistleblower", "SEC Whistleblower Program (Rule 21F)", 2, "governance"),
    ("admin_proceedings", "Administrative Proceedings", 2, "enforcement_sec"),
    ("civil_actions", "Civil Actions in Federal Court", 2, "enforcement_sec"),
    ("penalties", "Disgorgement and Civil Monetary Penalties", 2, "enforcement_sec"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_sec(conn) -> int:
    """Ingest SEC Regulations regulatory taxonomy.

    Returns 29 nodes.
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
    for code, title, level, parent in REG_SEC_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_SEC_NODES:
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
        for code, title, level, parent in REG_SEC_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_SEC_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
