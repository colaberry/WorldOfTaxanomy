"""FASB Standards regulatory taxonomy ingester.

Financial Accounting Standards Board - Accounting Standards Updates.
Authority: Financial Accounting Standards Board (FASB).
Source: https://www.fasb.org/standards

Data provenance: manual_transcription
License: Proprietary (FASB)

Total: 19 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_fasb"
_SYSTEM_NAME = "FASB Standards"
_FULL_NAME = "Financial Accounting Standards Board - Accounting Standards Updates"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "Financial Accounting Standards Board (FASB)"
_SOURCE_URL = "https://www.fasb.org/standards"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (FASB)"

# (code, title, level, parent_code)
REG_FASB_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("revenue", "Revenue Recognition Standards", 1, None),
    ("leases", "Lease Accounting Standards", 1, None),
    ("credit_losses", "Credit Losses Standards", 1, None),
    ("financial_inst", "Financial Instruments Standards", 1, None),
    ("disclosure", "Disclosure Framework Standards", 1, None),
    ("sustainability", "Sustainability Disclosure Standards", 1, None),
    ("asu_2014_09", "ASU 2014-09 - Revenue from Contracts with Customers (ASC 606)", 2, "revenue"),
    ("asu_2016_10", "ASU 2016-10 - Revenue: Identifying Performance Obligations", 2, "revenue"),
    ("asu_2016_12", "ASU 2016-12 - Revenue: Narrow-Scope Improvements", 2, "revenue"),
    ("asu_2016_02", "ASU 2016-02 - Leases (ASC 842)", 2, "leases"),
    ("asu_2020_05", "ASU 2020-05 - Leases: Effective Date Extension", 2, "leases"),
    ("asu_2016_13", "ASU 2016-13 - Current Expected Credit Losses (CECL)", 2, "credit_losses"),
    ("asu_2022_02", "ASU 2022-02 - TDR and Vintage Disclosures", 2, "credit_losses"),
    ("asu_2016_01", "ASU 2016-01 - Recognition and Measurement of Financial Assets", 2, "financial_inst"),
    ("asu_2020_04", "ASU 2020-04 - Reference Rate Reform (LIBOR Transition)", 2, "financial_inst"),
    ("asu_2023_01", "ASU 2023-01 - Investments - Equity Method and Joint Ventures", 2, "financial_inst"),
    ("asu_2023_07", "ASU 2023-07 - Segment Reporting Improvements", 2, "disclosure"),
    ("asu_2023_09", "ASU 2023-09 - Income Tax Disclosure Improvements", 2, "disclosure"),
    ("asu_2024_sustainability", "Climate-Related Disclosures (Proposed)", 2, "sustainability"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_fasb(conn) -> int:
    """Ingest FASB Standards regulatory taxonomy.

    Returns 19 nodes.
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
    for code, title, level, parent in REG_FASB_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FASB_NODES:
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
        for code, title, level, parent in REG_FASB_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FASB_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
