"""NERC CIP regulatory taxonomy ingester.

NERC Critical Infrastructure Protection Standards (CIP-002 through CIP-014).
Authority: North American Electric Reliability Corporation (NERC).
Source: https://www.nerc.com/pa/Stand/Pages/CIPStandards.aspx

Data provenance: manual_transcription
License: Public Domain

Total: 48 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_nerc_cip"
_SYSTEM_NAME = "NERC CIP"
_FULL_NAME = "NERC Critical Infrastructure Protection Standards (CIP-002 through CIP-014)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "North American Electric Reliability Corporation (NERC)"
_SOURCE_URL = "https://www.nerc.com/pa/Stand/Pages/CIPStandards.aspx"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_NERC_CIP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("cip_002", "CIP-002 - BES Cyber System Categorization", 1, None),
    ("cip_003", "CIP-003 - Security Management Controls", 1, None),
    ("cip_004", "CIP-004 - Personnel and Training", 1, None),
    ("cip_005", "CIP-005 - Electronic Security Perimeter(s)", 1, None),
    ("cip_006", "CIP-006 - Physical Security of BES Cyber Systems", 1, None),
    ("cip_007", "CIP-007 - System Security Management", 1, None),
    ("cip_008", "CIP-008 - Incident Reporting and Response Planning", 1, None),
    ("cip_009", "CIP-009 - Recovery Plans for BES Cyber Systems", 1, None),
    ("cip_010", "CIP-010 - Configuration Change Management and Vulnerability Assessments", 1, None),
    ("cip_011", "CIP-011 - Information Protection", 1, None),
    ("cip_012", "CIP-012 - Communications Between Control Centers", 1, None),
    ("cip_013", "CIP-013 - Supply Chain Risk Management", 1, None),
    ("cip_014", "CIP-014 - Physical Security", 1, None),
    ("cip_002_r1", "R1 - Identify and Categorize BES Cyber Systems", 2, "cip_002"),
    ("cip_002_r2", "R2 - Review and Approve Categorizations", 2, "cip_002"),
    ("cip_003_r1", "R1 - Cyber Security Policy", 2, "cip_003"),
    ("cip_003_r2", "R2 - Cyber Security Plans for Low Impact", 2, "cip_003"),
    ("cip_004_r1", "R1 - Security Awareness Program", 2, "cip_004"),
    ("cip_004_r2", "R2 - Training Program", 2, "cip_004"),
    ("cip_004_r3", "R3 - Personnel Risk Assessment", 2, "cip_004"),
    ("cip_004_r4", "R4 - Access Management Program", 2, "cip_004"),
    ("cip_005_r1", "R1 - Electronic Security Perimeter", 2, "cip_005"),
    ("cip_005_r2", "R2 - Remote Access Management", 2, "cip_005"),
    ("cip_006_r1", "R1 - Physical Security Plan", 2, "cip_006"),
    ("cip_006_r2", "R2 - Visitor Control Program", 2, "cip_006"),
    ("cip_006_r3", "R3 - Physical Access Control System Maintenance", 2, "cip_006"),
    ("cip_007_r1", "R1 - Ports and Services", 2, "cip_007"),
    ("cip_007_r2", "R2 - Security Patch Management", 2, "cip_007"),
    ("cip_007_r3", "R3 - Malicious Code Prevention", 2, "cip_007"),
    ("cip_007_r4", "R4 - Security Event Monitoring", 2, "cip_007"),
    ("cip_007_r5", "R5 - System Access Controls", 2, "cip_007"),
    ("cip_008_r1", "R1 - Cyber Security Incident Response Plan", 2, "cip_008"),
    ("cip_008_r2", "R2 - Cyber Security Incident Response Plan Implementation", 2, "cip_008"),
    ("cip_008_r3", "R3 - Cyber Security Incident Response Plan Review and Update", 2, "cip_008"),
    ("cip_009_r1", "R1 - Recovery Plans", 2, "cip_009"),
    ("cip_009_r2", "R2 - Recovery Plan Implementation and Testing", 2, "cip_009"),
    ("cip_009_r3", "R3 - Recovery Plan Review and Update", 2, "cip_009"),
    ("cip_010_r1", "R1 - Configuration Change Management", 2, "cip_010"),
    ("cip_010_r2", "R2 - Configuration Monitoring", 2, "cip_010"),
    ("cip_010_r3", "R3 - Vulnerability Assessments", 2, "cip_010"),
    ("cip_011_r1", "R1 - Information Protection", 2, "cip_011"),
    ("cip_011_r2", "R2 - BES Cyber System Information Disposal/Reuse", 2, "cip_011"),
    ("cip_012_r1", "R1 - Protection of Communication Links", 2, "cip_012"),
    ("cip_013_r1", "R1 - Supply Chain Cyber Security Risk Management Plan", 2, "cip_013"),
    ("cip_013_r2", "R2 - Vendor Risk Assessment", 2, "cip_013"),
    ("cip_014_r1", "R1 - Risk Assessment of Physical Threats", 2, "cip_014"),
    ("cip_014_r2", "R2 - Evaluation of Physical Threats", 2, "cip_014"),
    ("cip_014_r3", "R3 - Physical Security Plan", 2, "cip_014"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_nerc_cip(conn) -> int:
    """Ingest NERC CIP regulatory taxonomy.

    Returns 48 nodes.
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
    for code, title, level, parent in REG_NERC_CIP_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_NERC_CIP_NODES:
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
        for code, title, level, parent in REG_NERC_CIP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_NERC_CIP_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
