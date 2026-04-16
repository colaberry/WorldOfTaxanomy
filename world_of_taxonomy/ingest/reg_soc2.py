"""SOC 2 Trust Criteria regulatory taxonomy ingester.

SOC 2 Trust Services Criteria (AICPA 2017 with 2022 Updates).
Authority: American Institute of CPAs (AICPA).
Source: https://us.aicpa.org/interestareas/frc/assuranceadvisoryservices/trustdataintegritytaskforce

Data provenance: manual_transcription
License: Proprietary (AICPA)

Total: 37 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_soc2"
_SYSTEM_NAME = "SOC 2 Trust Criteria"
_FULL_NAME = "SOC 2 Trust Services Criteria (AICPA 2017 with 2022 Updates)"
_VERSION = "2022"
_REGION = "United States"
_AUTHORITY = "American Institute of CPAs (AICPA)"
_SOURCE_URL = "https://us.aicpa.org/interestareas/frc/assuranceadvisoryservices/trustdataintegritytaskforce"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (AICPA)"

# (code, title, level, parent_code)
REG_SOC2_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("cc_security", "CC - Common Criteria (Security)", 1, None),
    ("cc_availability", "A - Availability", 1, None),
    ("cc_processing", "PI - Processing Integrity", 1, None),
    ("cc_confidentiality", "C - Confidentiality", 1, None),
    ("cc_privacy", "P - Privacy", 1, None),
    ("cc1", "CC1 - Control Environment", 2, "cc_security"),
    ("cc2", "CC2 - Communication and Information", 2, "cc_security"),
    ("cc3", "CC3 - Risk Assessment", 2, "cc_security"),
    ("cc4", "CC4 - Monitoring Activities", 2, "cc_security"),
    ("cc5", "CC5 - Control Activities", 2, "cc_security"),
    ("cc6", "CC6 - Logical and Physical Access Controls", 2, "cc_security"),
    ("cc7", "CC7 - System Operations", 2, "cc_security"),
    ("cc8", "CC8 - Change Management", 2, "cc_security"),
    ("cc9", "CC9 - Risk Mitigation", 2, "cc_security"),
    ("cc1_1", "CC1.1 - Demonstrates Commitment to Integrity and Ethical Values", 3, "cc1"),
    ("cc1_2", "CC1.2 - Board of Directors Demonstrates Independence", 3, "cc1"),
    ("cc6_1", "CC6.1 - Logical Access Security Software and Infrastructure", 3, "cc6"),
    ("cc6_2", "CC6.2 - Prior to Issuing System Credentials and Granting Access", 3, "cc6"),
    ("cc6_3", "CC6.3 - Based on Authorization, Access to Data is Controlled", 3, "cc6"),
    ("cc6_6", "CC6.6 - Measures Against Threats Outside System Boundaries", 3, "cc6"),
    ("cc6_7", "CC6.7 - Transmission of Data Restricted to Authorized Users", 3, "cc6"),
    ("cc6_8", "CC6.8 - Prevent or Detect Unauthorized or Malicious Software", 3, "cc6"),
    ("cc7_1", "CC7.1 - Detect and Monitor for Security Events", 3, "cc7"),
    ("cc7_2", "CC7.2 - Monitor System Components for Anomalies", 3, "cc7"),
    ("cc7_3", "CC7.3 - Evaluate Security Events for Incidents", 3, "cc7"),
    ("cc7_4", "CC7.4 - Respond to Identified Security Incidents", 3, "cc7"),
    ("cc8_1", "CC8.1 - Changes to Infrastructure and Software Are Authorized", 3, "cc8"),
    ("a1_1", "A1.1 - System Availability Commitments and Requirements", 2, "cc_availability"),
    ("a1_2", "A1.2 - Environmental Protections and Recovery", 2, "cc_availability"),
    ("pi1_1", "PI1.1 - Processing Objectives Defined", 2, "cc_processing"),
    ("pi1_2", "PI1.2 - Inputs Validated for Completeness and Accuracy", 2, "cc_processing"),
    ("c1_1", "C1.1 - Confidential Information Identified and Protected", 2, "cc_confidentiality"),
    ("c1_2", "C1.2 - Confidential Information Disposed of Properly", 2, "cc_confidentiality"),
    ("p1_1", "P1.1 - Privacy Notice Provided", 2, "cc_privacy"),
    ("p2_1", "P2.1 - Choices for Collection and Use of Personal Information", 2, "cc_privacy"),
    ("p3_1", "P3.1 - Personal Information Collected for Identified Purposes", 2, "cc_privacy"),
    ("p4_1", "P4.1 - Personal Information Used for Identified Purposes", 2, "cc_privacy"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_soc2(conn) -> int:
    """Ingest SOC 2 Trust Criteria regulatory taxonomy.

    Returns 37 nodes.
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
    for code, title, level, parent in REG_SOC2_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_SOC2_NODES:
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
        for code, title, level, parent in REG_SOC2_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_SOC2_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
