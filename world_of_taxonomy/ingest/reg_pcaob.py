"""PCAOB Standards regulatory taxonomy ingester.

Public Company Accounting Oversight Board Auditing Standards.
Authority: Public Company Accounting Oversight Board (PCAOB).
Source: https://pcaobus.org/oversight/standards

Data provenance: manual_transcription
License: Public Domain

Total: 28 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_pcaob"
_SYSTEM_NAME = "PCAOB Standards"
_FULL_NAME = "Public Company Accounting Oversight Board Auditing Standards"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "Public Company Accounting Oversight Board (PCAOB)"
_SOURCE_URL = "https://pcaobus.org/oversight/standards"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_PCAOB_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("general", "General Auditing Standards", 1, None),
    ("risk_assess", "Risk Assessment Standards", 1, None),
    ("evidence", "Audit Evidence Standards", 1, None),
    ("reporting", "Reporting Standards", 1, None),
    ("other", "Other Standards", 1, None),
    ("as_1001", "AS 1001 - Responsibilities and Functions of the Independent Auditor", 2, "general"),
    ("as_1005", "AS 1005 - Independence", 2, "general"),
    ("as_1010", "AS 1010 - Training and Proficiency", 2, "general"),
    ("as_1015", "AS 1015 - Due Professional Care", 2, "general"),
    ("as_1105", "AS 1105 - Audit Evidence", 2, "general"),
    ("as_1201", "AS 1201 - Supervision of the Audit Engagement", 2, "general"),
    ("as_1301", "AS 1301 - Communications with Audit Committees", 2, "general"),
    ("as_2101", "AS 2101 - Audit Planning", 2, "risk_assess"),
    ("as_2105", "AS 2105 - Consideration of Materiality in Planning", 2, "risk_assess"),
    ("as_2110", "AS 2110 - Identifying and Assessing Risks of Material Misstatement", 2, "risk_assess"),
    ("as_2201", "AS 2201 - Audit of Internal Control Over Financial Reporting (ICFR)", 2, "risk_assess"),
    ("as_2301", "AS 2301 - Auditor's Responses to Risks of Material Misstatement", 2, "risk_assess"),
    ("as_2401", "AS 2401 - Consideration of Fraud", 2, "risk_assess"),
    ("as_2501", "AS 2501 - Auditing Accounting Estimates", 2, "evidence"),
    ("as_2502", "AS 2502 - Auditing Fair Value Measurements", 2, "evidence"),
    ("as_2503", "AS 2503 - Auditing Derivative Instruments", 2, "evidence"),
    ("as_2601", "AS 2601 - Using the Work of Other Auditors", 2, "evidence"),
    ("as_2605", "AS 2605 - Consideration of the Internal Audit Function", 2, "evidence"),
    ("as_3101", "AS 3101 - The Auditor's Report on an Audit (Critical Audit Matters)", 2, "reporting"),
    ("as_3105", "AS 3105 - Departures from Unqualified Opinions", 2, "reporting"),
    ("as_3110", "AS 3110 - Dating of the Independent Auditor's Report", 2, "reporting"),
    ("as_6101", "AS 6101 - Letters for Underwriters (Comfort Letters)", 2, "other"),
    ("as_6105", "AS 6105 - Reporting on Agreed-Upon Procedures", 2, "other"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_pcaob(conn) -> int:
    """Ingest PCAOB Standards regulatory taxonomy.

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
    for code, title, level, parent in REG_PCAOB_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_PCAOB_NODES:
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
        for code, title, level, parent in REG_PCAOB_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_PCAOB_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
