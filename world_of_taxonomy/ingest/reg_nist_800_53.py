"""NIST SP 800-53 Rev 5 regulatory taxonomy ingester.

NIST Special Publication 800-53 Revision 5 - Security and Privacy Controls.
Authority: National Institute of Standards and Technology (NIST).
Source: https://csrc.nist.gov/publications/detail/sp/800-53/rev-5/final

Data provenance: manual_transcription
License: Public Domain

Total: 36 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_nist_800_53"
_SYSTEM_NAME = "NIST SP 800-53 Rev 5"
_FULL_NAME = "NIST Special Publication 800-53 Revision 5 - Security and Privacy Controls"
_VERSION = "Rev 5"
_REGION = "United States"
_AUTHORITY = "National Institute of Standards and Technology (NIST)"
_SOURCE_URL = "https://csrc.nist.gov/publications/detail/sp/800-53/rev-5/final"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_NIST_800_53_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ac", "AC - Access Control", 1, None),
    ("at", "AT - Awareness and Training", 1, None),
    ("au", "AU - Audit and Accountability", 1, None),
    ("ca", "CA - Assessment, Authorization, and Monitoring", 1, None),
    ("cm", "CM - Configuration Management", 1, None),
    ("cp", "CP - Contingency Planning", 1, None),
    ("ia", "IA - Identification and Authentication", 1, None),
    ("ir", "IR - Incident Response", 1, None),
    ("ma", "MA - Maintenance", 1, None),
    ("mp", "MP - Media Protection", 1, None),
    ("pe", "PE - Physical and Environmental Protection", 1, None),
    ("pl", "PL - Planning", 1, None),
    ("pm", "PM - Program Management", 1, None),
    ("ps", "PS - Personnel Security", 1, None),
    ("pt", "PT - PII Processing and Transparency", 1, None),
    ("ra", "RA - Risk Assessment", 1, None),
    ("sa", "SA - System and Services Acquisition", 1, None),
    ("sc", "SC - System and Communications Protection", 1, None),
    ("si", "SI - System and Information Integrity", 1, None),
    ("sr", "SR - Supply Chain Risk Management", 1, None),
    ("ac_1", "AC-1 - Policy and Procedures", 2, "ac"),
    ("ac_2", "AC-2 - Account Management", 2, "ac"),
    ("ac_3", "AC-3 - Access Enforcement", 2, "ac"),
    ("ac_6", "AC-6 - Least Privilege", 2, "ac"),
    ("ac_17", "AC-17 - Remote Access", 2, "ac"),
    ("au_2", "AU-2 - Event Logging", 2, "au"),
    ("au_6", "AU-6 - Audit Record Review, Analysis, and Reporting", 2, "au"),
    ("cm_6", "CM-6 - Configuration Settings", 2, "cm"),
    ("ia_2", "IA-2 - Identification and Authentication (Organizational Users)", 2, "ia"),
    ("ia_5", "IA-5 - Authenticator Management", 2, "ia"),
    ("ir_4", "IR-4 - Incident Handling", 2, "ir"),
    ("ra_5", "RA-5 - Vulnerability Monitoring and Scanning", 2, "ra"),
    ("sc_7", "SC-7 - Boundary Protection", 2, "sc"),
    ("sc_8", "SC-8 - Transmission Confidentiality and Integrity", 2, "sc"),
    ("si_2", "SI-2 - Flaw Remediation", 2, "si"),
    ("si_4", "SI-4 - System Monitoring", 2, "si"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_nist_800_53(conn) -> int:
    """Ingest NIST SP 800-53 Rev 5 regulatory taxonomy.

    Returns 36 nodes.
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
    for code, title, level, parent in REG_NIST_800_53_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_NIST_800_53_NODES:
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
        for code, title, level, parent in REG_NIST_800_53_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_NIST_800_53_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
