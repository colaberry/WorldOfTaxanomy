"""PCI DSS v4.0 regulatory taxonomy ingester.

Payment Card Industry Data Security Standard Version 4.0.
Authority: PCI Security Standards Council.
Source: https://www.pcisecuritystandards.org/document_library/

Data provenance: manual_transcription
License: Proprietary (PCI SSC)

Total: 27 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_pci_dss"
_SYSTEM_NAME = "PCI DSS v4.0"
_FULL_NAME = "Payment Card Industry Data Security Standard Version 4.0"
_VERSION = "4.0"
_REGION = "Global"
_AUTHORITY = "PCI Security Standards Council"
_SOURCE_URL = "https://www.pcisecuritystandards.org/document_library/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (PCI SSC)"

# (code, title, level, parent_code)
REG_PCI_DSS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("req_1", "Requirement 1 - Install and Maintain Network Security Controls", 1, None),
    ("req_2", "Requirement 2 - Apply Secure Configurations to All System Components", 1, None),
    ("req_3", "Requirement 3 - Protect Stored Account Data", 1, None),
    ("req_4", "Requirement 4 - Protect Cardholder Data with Strong Cryptography During Transmission", 1, None),
    ("req_5", "Requirement 5 - Protect All Systems and Networks from Malicious Software", 1, None),
    ("req_6", "Requirement 6 - Develop and Maintain Secure Systems and Software", 1, None),
    ("req_7", "Requirement 7 - Restrict Access to System Components by Business Need to Know", 1, None),
    ("req_8", "Requirement 8 - Identify Users and Authenticate Access to System Components", 1, None),
    ("req_9", "Requirement 9 - Restrict Physical Access to Cardholder Data", 1, None),
    ("req_10", "Requirement 10 - Log and Monitor All Access to System Components and Cardholder Data", 1, None),
    ("req_11", "Requirement 11 - Test Security of Systems and Networks Regularly", 1, None),
    ("req_12", "Requirement 12 - Support Information Security with Organizational Policies and Programs", 1, None),
    ("req_1_2", "1.2 - Network Security Controls (NSC) Configuration", 2, "req_1"),
    ("req_1_3", "1.3 - Network Access to Cardholder Data Environment Restricted", 2, "req_1"),
    ("req_1_4", "1.4 - Network Connections Between Trusted and Untrusted Networks Controlled", 2, "req_1"),
    ("req_3_4", "3.4 - Access to Displays of Full PAN and Copies Restricted", 2, "req_3"),
    ("req_3_5", "3.5 - Primary Account Number (PAN) Secured Wherever Stored", 2, "req_3"),
    ("req_6_2", "6.2 - Bespoke and Custom Software Developed Securely", 2, "req_6"),
    ("req_6_3", "6.3 - Security Vulnerabilities Identified and Addressed", 2, "req_6"),
    ("req_6_4", "6.4 - Public-Facing Web Applications Protected Against Attacks", 2, "req_6"),
    ("req_8_3", "8.3 - Strong Authentication for Users and Administrators", 2, "req_8"),
    ("req_8_4", "8.4 - Multi-Factor Authentication (MFA) Implemented", 2, "req_8"),
    ("req_10_2", "10.2 - Audit Logs Implemented to Support Detection", 2, "req_10"),
    ("req_11_3", "11.3 - External and Internal Vulnerabilities Identified and Addressed", 2, "req_11"),
    ("req_11_4", "11.4 - External and Internal Penetration Testing Performed", 2, "req_11"),
    ("req_12_8", "12.8 - Risk to Information Assets from Third-Party Relationships Managed", 2, "req_12"),
    ("req_12_10", "12.10 - Suspected and Confirmed Security Incidents Responded to Immediately", 2, "req_12"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_pci_dss(conn) -> int:
    """Ingest PCI DSS v4.0 regulatory taxonomy.

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
    for code, title, level, parent in REG_PCI_DSS_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_PCI_DSS_NODES:
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
        for code, title, level, parent in REG_PCI_DSS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_PCI_DSS_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
