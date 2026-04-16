"""FAR regulatory taxonomy ingester.

Federal Acquisition Regulation (48 CFR Chapter 1).
Authority: GSA / DoD / NASA.
Source: https://www.ecfr.gov/current/title-48/chapter-1

Data provenance: manual_transcription
License: Public Domain

Total: 32 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_far"
_SYSTEM_NAME = "FAR"
_FULL_NAME = "Federal Acquisition Regulation (48 CFR Chapter 1)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "GSA / DoD / NASA"
_SOURCE_URL = "https://www.ecfr.gov/current/title-48/chapter-1"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FAR_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("part_1", "Part 1 - Federal Acquisition Regulations System", 1, None),
    ("part_2", "Part 2 - Definitions of Words and Terms", 1, None),
    ("part_3", "Part 3 - Improper Business Practices and Personal Conflicts of Interest", 1, None),
    ("part_4", "Part 4 - Administrative and Information Matters", 1, None),
    ("part_5", "Part 5 - Publicizing Contract Actions", 1, None),
    ("part_6", "Part 6 - Competition Requirements", 1, None),
    ("part_8", "Part 8 - Required Sources of Supplies and Services", 1, None),
    ("part_9", "Part 9 - Contractor Qualifications", 1, None),
    ("part_12", "Part 12 - Acquisition of Commercial Products and Services", 1, None),
    ("part_13", "Part 13 - Simplified Acquisition Procedures", 1, None),
    ("part_14", "Part 14 - Sealed Bidding", 1, None),
    ("part_15", "Part 15 - Contracting by Negotiation", 1, None),
    ("part_16", "Part 16 - Types of Contracts", 1, None),
    ("part_19", "Part 19 - Small Business Programs", 1, None),
    ("part_22", "Part 22 - Application of Labor Laws to Government Acquisitions", 1, None),
    ("part_23", "Part 23 - Environment, Energy, Water Efficiency, and Drug-Free Workplace", 1, None),
    ("part_25", "Part 25 - Foreign Acquisition (Buy American Act)", 1, None),
    ("part_27", "Part 27 - Patents, Data, and Copyrights", 1, None),
    ("part_28", "Part 28 - Bonds and Insurance", 1, None),
    ("part_30", "Part 30 - Cost Accounting Standards Administration", 1, None),
    ("part_31", "Part 31 - Contract Cost Principles and Procedures", 1, None),
    ("part_32", "Part 32 - Contract Financing", 1, None),
    ("part_33", "Part 33 - Protests, Disputes, and Appeals", 1, None),
    ("part_36", "Part 36 - Construction and Architect-Engineer Contracts", 1, None),
    ("part_37", "Part 37 - Service Contracting", 1, None),
    ("part_39", "Part 39 - Acquisition of Information Technology", 1, None),
    ("part_42", "Part 42 - Contract Administration and Audit Services", 1, None),
    ("part_45", "Part 45 - Government Property", 1, None),
    ("part_46", "Part 46 - Quality Assurance", 1, None),
    ("part_49", "Part 49 - Termination of Contracts", 1, None),
    ("part_52", "Part 52 - Solicitation Provisions and Contract Clauses", 1, None),
    ("part_53", "Part 53 - Forms", 1, None),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_far(conn) -> int:
    """Ingest FAR regulatory taxonomy.

    Returns 32 nodes.
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
    for code, title, level, parent in REG_FAR_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FAR_NODES:
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
        for code, title, level, parent in REG_FAR_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FAR_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
