"""CIS Controls v8 regulatory taxonomy ingester.

CIS Critical Security Controls Version 8.
Authority: Center for Internet Security (CIS).
Source: https://www.cisecurity.org/controls

Data provenance: manual_transcription
License: CC BY-NC-SA 4.0

Total: 29 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_cis_controls"
_SYSTEM_NAME = "CIS Controls v8"
_FULL_NAME = "CIS Critical Security Controls Version 8"
_VERSION = "8"
_REGION = "Global"
_AUTHORITY = "Center for Internet Security (CIS)"
_SOURCE_URL = "https://www.cisecurity.org/controls"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY-NC-SA 4.0"

# (code, title, level, parent_code)
REG_CIS_CONTROLS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ctrl_01", "Control 01 - Inventory and Control of Enterprise Assets", 1, None),
    ("ctrl_02", "Control 02 - Inventory and Control of Software Assets", 1, None),
    ("ctrl_03", "Control 03 - Data Protection", 1, None),
    ("ctrl_04", "Control 04 - Secure Configuration of Enterprise Assets and Software", 1, None),
    ("ctrl_05", "Control 05 - Account Management", 1, None),
    ("ctrl_06", "Control 06 - Access Control Management", 1, None),
    ("ctrl_07", "Control 07 - Continuous Vulnerability Management", 1, None),
    ("ctrl_08", "Control 08 - Audit Log Management", 1, None),
    ("ctrl_09", "Control 09 - Email and Web Browser Protections", 1, None),
    ("ctrl_10", "Control 10 - Malware Defenses", 1, None),
    ("ctrl_11", "Control 11 - Data Recovery", 1, None),
    ("ctrl_12", "Control 12 - Network Infrastructure Management", 1, None),
    ("ctrl_13", "Control 13 - Network Monitoring and Defense", 1, None),
    ("ctrl_14", "Control 14 - Security Awareness and Skills Training", 1, None),
    ("ctrl_15", "Control 15 - Service Provider Management", 1, None),
    ("ctrl_16", "Control 16 - Application Software Security", 1, None),
    ("ctrl_17", "Control 17 - Incident Response Management", 1, None),
    ("ctrl_18", "Control 18 - Penetration Testing", 1, None),
    ("ig_1", "Implementation Group 1 (IG1) - Essential Cyber Hygiene", 1, None),
    ("ig_2", "Implementation Group 2 (IG2) - Managed Security", 1, None),
    ("ig_3", "Implementation Group 3 (IG3) - Comprehensive Security", 1, None),
    ("sg_1_1", "1.1 - Establish and Maintain Detailed Enterprise Asset Inventory", 2, "ctrl_01"),
    ("sg_2_1", "2.1 - Establish and Maintain a Software Inventory", 2, "ctrl_02"),
    ("sg_3_1", "3.1 - Establish and Maintain a Data Management Process", 2, "ctrl_03"),
    ("sg_4_1", "4.1 - Establish and Maintain a Secure Configuration Process", 2, "ctrl_04"),
    ("sg_5_1", "5.1 - Establish and Maintain an Inventory of Accounts", 2, "ctrl_05"),
    ("sg_6_1", "6.1 - Establish an Access Granting Process", 2, "ctrl_06"),
    ("sg_7_1", "7.1 - Establish and Maintain a Vulnerability Management Process", 2, "ctrl_07"),
    ("sg_8_1", "8.1 - Establish and Maintain an Audit Log Management Process", 2, "ctrl_08"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_cis_controls(conn) -> int:
    """Ingest CIS Controls v8 regulatory taxonomy.

    Returns 29 nodes.
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
    for code, title, level, parent in REG_CIS_CONTROLS_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CIS_CONTROLS_NODES:
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
        for code, title, level, parent in REG_CIS_CONTROLS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CIS_CONTROLS_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
