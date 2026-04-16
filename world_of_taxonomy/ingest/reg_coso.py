"""COSO Framework regulatory taxonomy ingester.

COSO Internal Control - Integrated Framework (2013).
Authority: Committee of Sponsoring Organizations of the Treadway Commission.
Source: https://www.coso.org/guidance-on-ic

Data provenance: manual_transcription
License: Proprietary (COSO)

Total: 27 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_coso"
_SYSTEM_NAME = "COSO Framework"
_FULL_NAME = "COSO Internal Control - Integrated Framework (2013)"
_VERSION = "2013"
_REGION = "Global"
_AUTHORITY = "Committee of Sponsoring Organizations of the Treadway Commission"
_SOURCE_URL = "https://www.coso.org/guidance-on-ic"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (COSO)"

# (code, title, level, parent_code)
REG_COSO_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ctrl_env", "Control Environment", 1, None),
    ("risk_assess", "Risk Assessment", 1, None),
    ("ctrl_act", "Control Activities", 1, None),
    ("info_comm", "Information and Communication", 1, None),
    ("monitoring", "Monitoring Activities", 1, None),
    ("p1", "Principle 1 - Demonstrates Commitment to Integrity and Ethical Values", 2, "ctrl_env"),
    ("p2", "Principle 2 - Board of Directors Exercises Oversight Responsibility", 2, "ctrl_env"),
    ("p3", "Principle 3 - Establishes Structure, Authority, and Responsibility", 2, "ctrl_env"),
    ("p4", "Principle 4 - Demonstrates Commitment to Competence", 2, "ctrl_env"),
    ("p5", "Principle 5 - Enforces Accountability", 2, "ctrl_env"),
    ("p6", "Principle 6 - Specifies Suitable Objectives", 2, "risk_assess"),
    ("p7", "Principle 7 - Identifies and Analyzes Risk", 2, "risk_assess"),
    ("p8", "Principle 8 - Assesses Fraud Risk", 2, "risk_assess"),
    ("p9", "Principle 9 - Identifies and Analyzes Significant Change", 2, "risk_assess"),
    ("p10", "Principle 10 - Selects and Develops Control Activities", 2, "ctrl_act"),
    ("p11", "Principle 11 - Selects and Develops General Controls over Technology", 2, "ctrl_act"),
    ("p12", "Principle 12 - Deploys through Policies and Procedures", 2, "ctrl_act"),
    ("p13", "Principle 13 - Uses Relevant Information", 2, "info_comm"),
    ("p14", "Principle 14 - Communicates Internally", 2, "info_comm"),
    ("p15", "Principle 15 - Communicates Externally", 2, "info_comm"),
    ("p16", "Principle 16 - Conducts Ongoing and/or Separate Evaluations", 2, "monitoring"),
    ("p17", "Principle 17 - Evaluates and Communicates Deficiencies", 2, "monitoring"),
    ("erm_governance", "ERM - Governance and Culture", 1, None),
    ("erm_strategy", "ERM - Strategy and Objective-Setting", 1, None),
    ("erm_performance", "ERM - Performance", 1, None),
    ("erm_review", "ERM - Review and Revision", 1, None),
    ("erm_info", "ERM - Information, Communication, and Reporting", 1, None),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_coso(conn) -> int:
    """Ingest COSO Framework regulatory taxonomy.

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
    for code, title, level, parent in REG_COSO_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_COSO_NODES:
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
        for code, title, level, parent in REG_COSO_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_COSO_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
