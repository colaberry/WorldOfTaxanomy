"""FINRA Rules regulatory taxonomy ingester.

Financial Industry Regulatory Authority Rulebook.
Authority: Financial Industry Regulatory Authority (FINRA).
Source: https://www.finra.org/rules-guidance/rulebooks/finra-rules

Data provenance: manual_transcription
License: Public Domain

Total: 28 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_finra"
_SYSTEM_NAME = "FINRA Rules"
_FULL_NAME = "Financial Industry Regulatory Authority Rulebook"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "Financial Industry Regulatory Authority (FINRA)"
_SOURCE_URL = "https://www.finra.org/rules-guidance/rulebooks/finra-rules"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FINRA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("conduct", "Conduct Rules", 1, None),
    ("registration", "Registration and Qualification", 1, None),
    ("supervision", "Supervision and Compliance", 1, None),
    ("customer_protection", "Customer Protection", 1, None),
    ("market_integrity", "Market Integrity", 1, None),
    ("rule_2010", "Rule 2010 - Standards of Commercial Honor and Principles of Trade", 2, "conduct"),
    ("rule_2020", "Rule 2020 - Use of Manipulative, Deceptive or Other Fraudulent Devices", 2, "conduct"),
    ("rule_2090", "Rule 2090 - Know Your Customer", 2, "conduct"),
    ("rule_2111", "Rule 2111 - Suitability", 2, "conduct"),
    ("rule_2121", "Rule 2121 - Fair Prices and Commissions", 2, "conduct"),
    ("rule_2150", "Rule 2150 - Improper Use of Customers' Securities or Funds", 2, "conduct"),
    ("rule_2210", "Rule 2210 - Communications with the Public", 2, "conduct"),
    ("rule_1010", "Rule 1010 Series - Membership Application Procedures", 2, "registration"),
    ("rule_1122", "Rule 1122 - Filing of Misleading Information", 2, "registration"),
    ("rule_1240", "Rule 1240 - Continuing Education Requirements", 2, "registration"),
    ("rule_3110", "Rule 3110 - Supervision", 2, "supervision"),
    ("rule_3120", "Rule 3120 - Supervisory Control System", 2, "supervision"),
    ("rule_3130", "Rule 3130 - Annual Certification of Compliance", 2, "supervision"),
    ("rule_3310", "Rule 3310 - Anti-Money Laundering Compliance Program", 2, "supervision"),
    ("rule_4210", "Rule 4210 - Margin Requirements", 2, "customer_protection"),
    ("rule_4370", "Rule 4370 - Business Continuity Plans", 2, "customer_protection"),
    ("rule_4512", "Rule 4512 - Customer Account Information", 2, "customer_protection"),
    ("rule_4530", "Rule 4530 - Reporting Requirements", 2, "customer_protection"),
    ("rule_5110", "Rule 5110 - Corporate Financing Rule (Underwriting)", 2, "market_integrity"),
    ("rule_5130", "Rule 5130 - Restrictions on Spinning and Flipping of IPOs", 2, "market_integrity"),
    ("rule_5210", "Rule 5210 - Publication of Transactions and Quotations", 2, "market_integrity"),
    ("rule_5310", "Rule 5310 - Best Execution and Interpositioning", 2, "market_integrity"),
    ("rule_6100", "Rule 6100 Series - Quoting and Trading in NMS Stocks", 2, "market_integrity"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_finra(conn) -> int:
    """Ingest FINRA Rules regulatory taxonomy.

    Returns 28 nodes.
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
    for code, title, level, parent in REG_FINRA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FINRA_NODES:
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
        for code, title, level, parent in REG_FINRA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FINRA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
