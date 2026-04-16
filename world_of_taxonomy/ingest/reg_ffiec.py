"""FFIEC IT Handbook regulatory taxonomy ingester.

Federal Financial Institutions Examination Council IT Examination Handbook.
Authority: FFIEC (Fed, FDIC, OCC, NCUA, CFPB).
Source: https://ithandbook.ffiec.gov/

Data provenance: manual_transcription
License: Public Domain

Total: 25 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_ffiec"
_SYSTEM_NAME = "FFIEC IT Handbook"
_FULL_NAME = "Federal Financial Institutions Examination Council IT Examination Handbook"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "FFIEC (Fed, FDIC, OCC, NCUA, CFPB)"
_SOURCE_URL = "https://ithandbook.ffiec.gov/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FFIEC_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("audit", "Audit Booklet", 1, None),
    ("bcp", "Business Continuity Management Booklet", 1, None),
    ("dev_acq", "Development, Acquisition, and Maintenance Booklet", 1, None),
    ("ebanking", "E-Banking Booklet", 1, None),
    ("info_sec", "Information Security Booklet", 1, None),
    ("mgmt", "Management Booklet", 1, None),
    ("ops", "Operations Booklet", 1, None),
    ("outsource", "Outsourcing Technology Services Booklet", 1, None),
    ("payments", "Retail Payment Systems Booklet", 1, None),
    ("supervision", "Supervision of Technology Service Providers Booklet", 1, None),
    ("wholesale", "Wholesale Payment Systems Booklet", 1, None),
    ("arch", "Architecture, Infrastructure, and Operations Booklet", 1, None),
    ("is_governance", "Information Security Governance", 2, "info_sec"),
    ("is_risk", "Risk Assessment", 2, "info_sec"),
    ("is_controls", "Security Controls", 2, "info_sec"),
    ("is_monitoring", "Security Monitoring", 2, "info_sec"),
    ("is_incident", "Incident Response", 2, "info_sec"),
    ("is_testing", "Security Testing", 2, "info_sec"),
    ("bcp_bia", "Business Impact Analysis", 2, "bcp"),
    ("bcp_risk", "Risk Assessment", 2, "bcp"),
    ("bcp_strategy", "Recovery Strategy", 2, "bcp"),
    ("bcp_testing", "Testing and Exercises", 2, "bcp"),
    ("cat_tool", "Cybersecurity Assessment Tool (CAT)", 1, None),
    ("cat_inherent", "Inherent Risk Profile (CAT)", 2, "cat_tool"),
    ("cat_maturity", "Cybersecurity Maturity (CAT)", 2, "cat_tool"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_ffiec(conn) -> int:
    """Ingest FFIEC IT Handbook regulatory taxonomy.

    Returns 25 nodes.
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
    for code, title, level, parent in REG_FFIEC_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FFIEC_NODES:
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
        for code, title, level, parent in REG_FFIEC_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FFIEC_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
