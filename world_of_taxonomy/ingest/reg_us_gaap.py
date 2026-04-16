"""US GAAP (ASC) regulatory taxonomy ingester.

US Generally Accepted Accounting Principles - FASB Accounting Standards Codification.
Authority: Financial Accounting Standards Board (FASB).
Source: https://asc.fasb.org/

Data provenance: manual_transcription
License: Proprietary (FASB)

Total: 33 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_us_gaap"
_SYSTEM_NAME = "US GAAP (ASC)"
_FULL_NAME = "US Generally Accepted Accounting Principles - FASB Accounting Standards Codification"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "Financial Accounting Standards Board (FASB)"
_SOURCE_URL = "https://asc.fasb.org/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (FASB)"

# (code, title, level, parent_code)
REG_US_GAAP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("general", "General Principles (ASC 100-199)", 1, None),
    ("presentation", "Presentation (ASC 200-299)", 1, None),
    ("assets", "Assets (ASC 300-399)", 1, None),
    ("liabilities", "Liabilities (ASC 400-499)", 1, None),
    ("equity", "Equity (ASC 500-599)", 1, None),
    ("revenue", "Revenue (ASC 600-699)", 1, None),
    ("expenses", "Expenses (ASC 700-799)", 1, None),
    ("broad_trans", "Broad Transactions (ASC 800-899)", 1, None),
    ("industry", "Industry (ASC 900-999)", 1, None),
    ("asc_205", "ASC 205 - Presentation of Financial Statements", 2, "presentation"),
    ("asc_230", "ASC 230 - Statement of Cash Flows", 2, "presentation"),
    ("asc_260", "ASC 260 - Earnings Per Share", 2, "presentation"),
    ("asc_310", "ASC 310 - Receivables", 2, "assets"),
    ("asc_320", "ASC 320 - Investments - Debt Securities", 2, "assets"),
    ("asc_330", "ASC 330 - Inventory", 2, "assets"),
    ("asc_340", "ASC 340 - Other Assets and Deferred Costs", 2, "assets"),
    ("asc_350", "ASC 350 - Intangibles - Goodwill and Other", 2, "assets"),
    ("asc_360", "ASC 360 - Property, Plant, and Equipment", 2, "assets"),
    ("asc_410", "ASC 410 - Asset Retirement and Environmental Obligations", 2, "liabilities"),
    ("asc_420", "ASC 420 - Exit or Disposal Cost Obligations", 2, "liabilities"),
    ("asc_450", "ASC 450 - Contingencies", 2, "liabilities"),
    ("asc_470", "ASC 470 - Debt", 2, "liabilities"),
    ("asc_480", "ASC 480 - Distinguishing Liabilities from Equity", 2, "liabilities"),
    ("asc_505", "ASC 505 - Equity", 2, "equity"),
    ("asc_606", "ASC 606 - Revenue from Contracts with Customers", 2, "revenue"),
    ("asc_718", "ASC 718 - Compensation - Stock Compensation", 2, "expenses"),
    ("asc_740", "ASC 740 - Income Taxes", 2, "expenses"),
    ("asc_805", "ASC 805 - Business Combinations", 2, "broad_trans"),
    ("asc_810", "ASC 810 - Consolidation", 2, "broad_trans"),
    ("asc_815", "ASC 815 - Derivatives and Hedging", 2, "broad_trans"),
    ("asc_820", "ASC 820 - Fair Value Measurement", 2, "broad_trans"),
    ("asc_842", "ASC 842 - Leases", 2, "broad_trans"),
    ("asc_860", "ASC 860 - Transfers and Servicing", 2, "broad_trans"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_us_gaap(conn) -> int:
    """Ingest US GAAP (ASC) regulatory taxonomy.

    Returns 33 nodes.
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
    for code, title, level, parent in REG_US_GAAP_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_US_GAAP_NODES:
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
        for code, title, level, parent in REG_US_GAAP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_US_GAAP_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
