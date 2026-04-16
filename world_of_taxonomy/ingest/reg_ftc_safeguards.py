"""FTC Safeguards Rule regulatory taxonomy ingester.

FTC Standards for Safeguarding Customer Information (16 CFR Part 314).
Authority: Federal Trade Commission (FTC).
Source: https://www.ecfr.gov/current/title-16/chapter-I/subchapter-C/part-314

Data provenance: manual_transcription
License: Public Domain

Total: 23 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_ftc_safeguards"
_SYSTEM_NAME = "FTC Safeguards Rule"
_FULL_NAME = "FTC Standards for Safeguarding Customer Information (16 CFR Part 314)"
_VERSION = "2023"
_REGION = "United States"
_AUTHORITY = "Federal Trade Commission (FTC)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-16/chapter-I/subchapter-C/part-314"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FTC_SAFEGUARDS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Definitions", 1, None),
    ("program", "Information Security Program Requirements", 1, None),
    ("safeguards", "Required Safeguards", 1, None),
    ("oversight", "Oversight and Reporting", 1, None),
    ("def_fi", "Definition of Financial Institution (314.2(h))", 2, "scope"),
    ("def_customer_info", "Definition of Customer Information (314.2(d))", 2, "scope"),
    ("qualified_individual", "Designated Qualified Individual (314.4(a))", 2, "program"),
    ("risk_assessment", "Written Risk Assessment (314.4(b))", 2, "program"),
    ("written_program", "Written Information Security Program", 2, "program"),
    ("access_controls", "Access Controls (314.4(c)(1))", 2, "safeguards"),
    ("inventory", "Information Asset Inventory (314.4(c)(2))", 2, "safeguards"),
    ("encryption", "Encryption of Customer Information (314.4(c)(3))", 2, "safeguards"),
    ("mfa", "Multi-Factor Authentication (314.4(c)(5))", 2, "safeguards"),
    ("data_disposal", "Secure Disposal of Customer Information (314.4(c)(6))", 2, "safeguards"),
    ("change_mgmt", "Change Management Procedures (314.4(c)(7))", 2, "safeguards"),
    ("monitoring", "Monitoring and Logging (314.4(c)(8))", 2, "safeguards"),
    ("vulnerability", "Vulnerability Assessment (314.4(d))", 2, "safeguards"),
    ("pen_test", "Penetration Testing (314.4(d)(2))", 2, "safeguards"),
    ("service_provider", "Service Provider Oversight (314.4(f))", 2, "safeguards"),
    ("incident_response", "Incident Response Plan (314.4(h))", 2, "safeguards"),
    ("qi_report", "Qualified Individual Reporting to Board (314.4(i))", 2, "oversight"),
    ("board_report", "Annual Board Report", 2, "oversight"),
    ("notification", "FTC Notification Requirements (314.4(j))", 2, "oversight"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_ftc_safeguards(conn) -> int:
    """Ingest FTC Safeguards Rule regulatory taxonomy.

    Returns 23 nodes.
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
    for code, title, level, parent in REG_FTC_SAFEGUARDS_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FTC_SAFEGUARDS_NODES:
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
        for code, title, level, parent in REG_FTC_SAFEGUARDS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FTC_SAFEGUARDS_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
