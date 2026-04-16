"""NIST SP 800-171 Rev 3 regulatory taxonomy ingester.

NIST Special Publication 800-171 Revision 3 - Protecting Controlled Unclassified Information.
Authority: National Institute of Standards and Technology (NIST).
Source: https://csrc.nist.gov/publications/detail/sp/800-171/rev-3/final

Data provenance: manual_transcription
License: Public Domain

Total: 28 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_nist_800_171"
_SYSTEM_NAME = "NIST SP 800-171 Rev 3"
_FULL_NAME = "NIST Special Publication 800-171 Revision 3 - Protecting Controlled Unclassified Information"
_VERSION = "Rev 3"
_REGION = "United States"
_AUTHORITY = "National Institute of Standards and Technology (NIST)"
_SOURCE_URL = "https://csrc.nist.gov/publications/detail/sp/800-171/rev-3/final"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_NIST_800_171_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ac", "Access Control (03.01)", 1, None),
    ("at", "Awareness and Training (03.02)", 1, None),
    ("au", "Audit and Accountability (03.03)", 1, None),
    ("ca", "Assessment and Monitoring (03.04)", 1, None),
    ("cm", "Configuration Management (03.05)", 1, None),
    ("ia", "Identification and Authentication (03.06)", 1, None),
    ("ir", "Incident Response (03.07)", 1, None),
    ("ma", "Maintenance (03.08)", 1, None),
    ("mp", "Media Protection (03.09)", 1, None),
    ("pe", "Physical Protection (03.10)", 1, None),
    ("ps", "Personnel Security (03.11)", 1, None),
    ("ra", "Risk Assessment (03.12)", 1, None),
    ("sc", "System and Communications Protection (03.13)", 1, None),
    ("si", "System and Information Integrity (03.14)", 1, None),
    ("ac_l_01", "03.01.01 - Account Management", 2, "ac"),
    ("ac_l_02", "03.01.02 - Access Enforcement", 2, "ac"),
    ("ac_l_05", "03.01.05 - Least Privilege", 2, "ac"),
    ("ac_l_12", "03.01.12 - Remote Access", 2, "ac"),
    ("ac_l_18", "03.01.18 - Access Control for Mobile Devices", 2, "ac"),
    ("au_l_01", "03.03.01 - Event Logging", 2, "au"),
    ("au_l_02", "03.03.02 - Audit Record Content", 2, "au"),
    ("cm_l_01", "03.05.01 - Baseline Configuration", 2, "cm"),
    ("cm_l_02", "03.05.02 - Configuration Change Control", 2, "cm"),
    ("ia_l_01", "03.06.01 - User Identification and Authentication", 2, "ia"),
    ("ia_l_05", "03.06.05 - Authenticator Management", 2, "ia"),
    ("ir_l_02", "03.07.02 - Incident Reporting", 2, "ir"),
    ("sc_l_01", "03.13.01 - Boundary Protection", 2, "sc"),
    ("si_l_02", "03.14.02 - Flaw Remediation", 2, "si"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_nist_800_171(conn) -> int:
    """Ingest NIST SP 800-171 Rev 3 regulatory taxonomy.

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
    for code, title, level, parent in REG_NIST_800_171_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_NIST_800_171_NODES:
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
        for code, title, level, parent in REG_NIST_800_171_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_NIST_800_171_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
