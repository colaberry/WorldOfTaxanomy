"""FCRA regulatory taxonomy ingester.

Fair Credit Reporting Act (15 USC 1681 et seq.).
Authority: US Congress / CFPB / FTC.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title15/html/USCODE-2023-title15-chap41-subchapIII.htm

Data provenance: manual_transcription
License: Public Domain

Total: 27 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_fcra"
_SYSTEM_NAME = "FCRA"
_FULL_NAME = "Fair Credit Reporting Act (15 USC 1681 et seq.)"
_VERSION = "1970"
_REGION = "United States"
_AUTHORITY = "US Congress / CFPB / FTC"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title15/html/USCODE-2023-title15-chap41-subchapIII.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FCRA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("purpose", "Findings, Purpose, and Definitions", 1, None),
    ("consumer_reports", "Consumer Reports and Permissible Purposes", 1, None),
    ("agency_duties", "Consumer Reporting Agency Duties", 1, None),
    ("furnisher_duties", "Furnisher Responsibilities", 1, None),
    ("consumer_rights", "Consumer Rights", 1, None),
    ("enforcement", "Enforcement and Penalties", 1, None),
    ("sec_602", "Sec 602 - Congressional Findings and Purpose", 2, "purpose"),
    ("sec_603", "Sec 603 - Definitions (Consumer Report, CRA, etc.)", 2, "purpose"),
    ("sec_604", "Sec 604 - Permissible Purposes of Consumer Reports", 2, "consumer_reports"),
    ("sec_604b", "Sec 604(b) - Use by Employers", 2, "consumer_reports"),
    ("sec_605", "Sec 605 - Requirements on Information in Reports", 2, "consumer_reports"),
    ("sec_605a", "Sec 605A - Identity Theft Prevention - Fraud Alerts", 2, "consumer_reports"),
    ("sec_605b", "Sec 605B - Block of Information from Identity Theft", 2, "consumer_reports"),
    ("sec_607", "Sec 607 - Compliance Procedures", 2, "agency_duties"),
    ("sec_609", "Sec 609 - Disclosures to Consumers", 2, "agency_duties"),
    ("sec_610", "Sec 610 - Conditions for Disclosure to Consumers", 2, "agency_duties"),
    ("sec_611", "Sec 611 - Procedure for Disputed Information", 2, "agency_duties"),
    ("sec_612", "Sec 612 - Charges for Disclosures", 2, "agency_duties"),
    ("sec_623", "Sec 623 - Responsibilities of Furnishers of Information", 2, "furnisher_duties"),
    ("sec_624", "Sec 624 - Affiliate Sharing Notices", 2, "furnisher_duties"),
    ("sec_615", "Sec 615 - Requirements on Users Taking Adverse Actions", 2, "consumer_rights"),
    ("sec_616", "Sec 616 - Civil Liability for Willful Noncompliance", 2, "consumer_rights"),
    ("sec_617", "Sec 617 - Civil Liability for Negligent Noncompliance", 2, "consumer_rights"),
    ("sec_621", "Sec 621 - Administrative Enforcement (CFPB/FTC)", 2, "enforcement"),
    ("sec_625", "Sec 625 - Relation to State Laws", 2, "enforcement"),
    ("sec_626", "Sec 626 - Unauthorized Disclosures by Officers or Employees", 2, "enforcement"),
    ("sec_628", "Sec 628 - Disposal of Consumer Report Information", 2, "enforcement"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_fcra(conn) -> int:
    """Ingest FCRA regulatory taxonomy.

    Returns 27 nodes.
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
    for code, title, level, parent in REG_FCRA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FCRA_NODES:
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
        for code, title, level, parent in REG_FCRA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FCRA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
