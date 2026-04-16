"""CFPB Regulations regulatory taxonomy ingester.

Consumer Financial Protection Bureau Regulations (12 CFR Chapter X).
Authority: Consumer Financial Protection Bureau (CFPB).
Source: https://www.ecfr.gov/current/title-12/chapter-X

Data provenance: manual_transcription
License: Public Domain

Total: 22 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_cfpb"
_SYSTEM_NAME = "CFPB Regulations"
_FULL_NAME = "Consumer Financial Protection Bureau Regulations (12 CFR Chapter X)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "Consumer Financial Protection Bureau (CFPB)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-12/chapter-X"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_CFPB_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("lending", "Lending and Credit Regulations", 1, None),
    ("consumer_protection", "Consumer Protection Regulations", 1, None),
    ("payments", "Payment and Account Regulations", 1, None),
    ("reporting", "Reporting and Disclosure Regulations", 1, None),
    ("collections", "Debt Collection Regulations", 1, None),
    ("reg_b", "Regulation B - Equal Credit Opportunity (12 CFR 1002)", 2, "lending"),
    ("reg_c", "Regulation C - Home Mortgage Disclosure (HMDA) (12 CFR 1003)", 2, "lending"),
    ("reg_x", "Regulation X - Real Estate Settlement Procedures (RESPA) (12 CFR 1024)", 2, "lending"),
    ("reg_z", "Regulation Z - Truth in Lending (TILA) (12 CFR 1026)", 2, "lending"),
    ("higher_priced", "Higher-Priced Mortgage Loan Rules", 2, "lending"),
    ("ability_to_repay", "Ability-to-Repay / Qualified Mortgage (ATR/QM)", 2, "lending"),
    ("reg_e", "Regulation E - Electronic Fund Transfers (12 CFR 1005)", 2, "consumer_protection"),
    ("reg_dd", "Regulation DD - Truth in Savings (12 CFR 1030)", 2, "consumer_protection"),
    ("reg_p", "Regulation P - Privacy of Consumer Financial Information (12 CFR 1016)", 2, "consumer_protection"),
    ("udaap", "UDAAP - Unfair, Deceptive, or Abusive Acts or Practices", 2, "consumer_protection"),
    ("reg_ii", "Regulation II - Debit Card Interchange (Durbin Amendment) (12 CFR 1005)", 2, "payments"),
    ("prepaid", "Prepaid Account Rule (12 CFR 1005 Subpart B)", 2, "payments"),
    ("remittances", "Remittance Transfer Rule (12 CFR 1005 Subpart B)", 2, "payments"),
    ("reg_v", "Regulation V - Fair Credit Reporting (12 CFR 1022)", 2, "reporting"),
    ("small_biz_1071", "Section 1071 - Small Business Lending Data Collection", 2, "reporting"),
    ("reg_f", "Regulation F - Fair Debt Collection Practices (12 CFR 1006)", 2, "collections"),
    ("fdcpa_rules", "FDCPA Implementing Rules", 2, "collections"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_cfpb(conn) -> int:
    """Ingest CFPB Regulations regulatory taxonomy.

    Returns 22 nodes.
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
    for code, title, level, parent in REG_CFPB_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CFPB_NODES:
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
        for code, title, level, parent in REG_CFPB_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CFPB_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
