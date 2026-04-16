"""GLBA regulatory taxonomy ingester.

Gramm-Leach-Bliley Act of 1999.
Authority: US Congress / FTC / Banking Regulators.
Source: https://www.govinfo.gov/content/pkg/PLAW-106publ102/html/PLAW-106publ102.htm

Data provenance: manual_transcription
License: Public Domain

Total: 28 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_glba"
_SYSTEM_NAME = "GLBA"
_FULL_NAME = "Gramm-Leach-Bliley Act of 1999"
_VERSION = "1999"
_REGION = "United States"
_AUTHORITY = "US Congress / FTC / Banking Regulators"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/PLAW-106publ102/html/PLAW-106publ102.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_GLBA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Facilitating Affiliation Among Banks, Securities Firms, and Insurance Companies", 1, None),
    ("title_2", "Title II - Functional Regulation", 1, None),
    ("title_3", "Title III - Insurance", 1, None),
    ("title_4", "Title IV - Unitary Savings and Loan Holding Companies", 1, None),
    ("title_5", "Title V - Privacy", 1, None),
    ("sec_101", "Sec 101 - Financial Holding Company Structure", 2, "title_1"),
    ("sec_102", "Sec 102 - Activities of Financial Holding Companies", 2, "title_1"),
    ("sec_103", "Sec 103 - Conforming Amendments", 2, "title_1"),
    ("sec_201", "Sec 201 - Broker-Dealer Functions in Banks", 2, "title_2"),
    ("sec_202", "Sec 202 - Securities Activities of Banks", 2, "title_2"),
    ("sec_211", "Sec 211 - Investment Advisers Act Amendments", 2, "title_2"),
    ("sec_301", "Sec 301 - Functional Regulation of Insurance", 2, "title_3"),
    ("sec_302", "Sec 302 - State Insurance Regulation", 2, "title_3"),
    ("sec_303", "Sec 303 - Nondiscrimination for Insurance Sales", 2, "title_3"),
    ("sec_401", "Sec 401 - Unitary Thrift Holding Company Provisions", 2, "title_4"),
    ("sub_a", "Subtitle A - Disclosure of Nonpublic Personal Information", 2, "title_5"),
    ("sub_b", "Subtitle B - Fraudulent Access to Financial Information (Pretexting)", 2, "title_5"),
    ("sec_501", "Sec 501 - Protection of Nonpublic Personal Information", 3, "sub_a"),
    ("sec_502", "Sec 502 - Obligations with Respect to Disclosures", 3, "sub_a"),
    ("sec_503", "Sec 503 - Disclosure of Institution Privacy Policy", 3, "sub_a"),
    ("sec_504", "Sec 504 - Rulemaking", 3, "sub_a"),
    ("sec_505", "Sec 505 - State Laws", 3, "sub_a"),
    ("sec_507", "Sec 507 - Opt-Out Right for Financial Information", 3, "sub_a"),
    ("sec_509", "Sec 509 - Definitions", 3, "sub_a"),
    ("sec_521", "Sec 521 - Privacy Protection for Customer Information of Financial Institutions", 3, "sub_b"),
    ("sec_522", "Sec 522 - Prohibitions on Obtaining Customer Information by False Pretenses", 3, "sub_b"),
    ("reg_p", "Regulation P (12 CFR Part 1016) - Privacy of Consumer Financial Information", 2, "title_5"),
    ("safeguards_rule", "FTC Safeguards Rule (16 CFR Part 314)", 2, "title_5"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_glba(conn) -> int:
    """Ingest GLBA regulatory taxonomy.

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
    for code, title, level, parent in REG_GLBA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_GLBA_NODES:
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
        for code, title, level, parent in REG_GLBA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_GLBA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
