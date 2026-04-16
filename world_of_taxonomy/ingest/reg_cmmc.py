"""CMMC 2.0 regulatory taxonomy ingester.

Cybersecurity Maturity Model Certification 2.0.
Authority: US Department of Defense (DoD).
Source: https://dodcio.defense.gov/CMMC/

Data provenance: manual_transcription
License: Public Domain

Total: 25 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_cmmc"
_SYSTEM_NAME = "CMMC 2.0"
_FULL_NAME = "Cybersecurity Maturity Model Certification 2.0"
_VERSION = "2.0"
_REGION = "United States"
_AUTHORITY = "US Department of Defense (DoD)"
_SOURCE_URL = "https://dodcio.defense.gov/CMMC/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_CMMC_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("level_1", "Level 1 - Foundational (17 Practices)", 1, None),
    ("level_2", "Level 2 - Advanced (110 Practices, NIST SP 800-171)", 1, None),
    ("level_3", "Level 3 - Expert (110+ Practices, NIST SP 800-172)", 1, None),
    ("l1_ac", "AC - Access Control (Level 1)", 2, "level_1"),
    ("l1_ia", "IA - Identification and Authentication (Level 1)", 2, "level_1"),
    ("l1_mp", "MP - Media Protection (Level 1)", 2, "level_1"),
    ("l1_pe", "PE - Physical Protection (Level 1)", 2, "level_1"),
    ("l1_sc", "SC - System and Communications Protection (Level 1)", 2, "level_1"),
    ("l1_si", "SI - System and Information Integrity (Level 1)", 2, "level_1"),
    ("l2_ac", "AC - Access Control (Level 2)", 2, "level_2"),
    ("l2_at", "AT - Awareness and Training (Level 2)", 2, "level_2"),
    ("l2_au", "AU - Audit and Accountability (Level 2)", 2, "level_2"),
    ("l2_ca", "CA - Assessment, Authorization (Level 2)", 2, "level_2"),
    ("l2_cm", "CM - Configuration Management (Level 2)", 2, "level_2"),
    ("l2_ia", "IA - Identification and Authentication (Level 2)", 2, "level_2"),
    ("l2_ir", "IR - Incident Response (Level 2)", 2, "level_2"),
    ("l2_ma", "MA - Maintenance (Level 2)", 2, "level_2"),
    ("l2_mp", "MP - Media Protection (Level 2)", 2, "level_2"),
    ("l2_pe", "PE - Physical Protection (Level 2)", 2, "level_2"),
    ("l2_ps", "PS - Personnel Security (Level 2)", 2, "level_2"),
    ("l2_ra", "RA - Risk Assessment (Level 2)", 2, "level_2"),
    ("l2_sc", "SC - System and Communications Protection (Level 2)", 2, "level_2"),
    ("l2_si", "SI - System and Information Integrity (Level 2)", 2, "level_2"),
    ("l3_enhanced", "Enhanced Security Requirements (NIST SP 800-172)", 2, "level_3"),
    ("l3_apt", "Advanced Persistent Threat Protections", 2, "level_3"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_cmmc(conn) -> int:
    """Ingest CMMC 2.0 regulatory taxonomy.

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
    for code, title, level, parent in REG_CMMC_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CMMC_NODES:
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
        for code, title, level, parent in REG_CMMC_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CMMC_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
