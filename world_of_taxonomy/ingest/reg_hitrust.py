"""HITRUST CSF regulatory taxonomy ingester.

HITRUST Common Security Framework v11.
Authority: HITRUST Alliance.
Source: https://hitrustalliance.net/hitrust-csf/

Data provenance: manual_transcription
License: Proprietary (HITRUST)

Total: 27 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_hitrust"
_SYSTEM_NAME = "HITRUST CSF"
_FULL_NAME = "HITRUST Common Security Framework v11"
_VERSION = "11"
_REGION = "United States"
_AUTHORITY = "HITRUST Alliance"
_SOURCE_URL = "https://hitrustalliance.net/hitrust-csf/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (HITRUST)"

# (code, title, level, parent_code)
REG_HITRUST_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("cat_0", "Category 0 - Information Security Management Program", 1, None),
    ("cat_1", "Category 1 - Access Control", 1, None),
    ("cat_2", "Category 2 - Human Resources Security", 1, None),
    ("cat_3", "Category 3 - Risk Management", 1, None),
    ("cat_4", "Category 4 - Security Policy", 1, None),
    ("cat_5", "Category 5 - Organization of Information Security", 1, None),
    ("cat_6", "Category 6 - Compliance", 1, None),
    ("cat_7", "Category 7 - Asset Management", 1, None),
    ("cat_8", "Category 8 - Physical and Environmental Security", 1, None),
    ("cat_9", "Category 9 - Communications and Operations Management", 1, None),
    ("cat_10", "Category 10 - Information Systems Acquisition, Development, and Maintenance", 1, None),
    ("cat_11", "Category 11 - Information Security Incident Management", 1, None),
    ("cat_12", "Category 12 - Business Continuity Management", 1, None),
    ("cat_13", "Category 13 - Privacy Practices", 1, None),
    ("obj_01_a", "01.a - Access Control Policy", 2, "cat_1"),
    ("obj_01_b", "01.b - User Registration", 2, "cat_1"),
    ("obj_01_d", "01.d - User Password Management", 2, "cat_1"),
    ("obj_01_j", "01.j - Access Control to Program Source Code", 2, "cat_1"),
    ("obj_01_v", "01.v - Information Access Restriction", 2, "cat_1"),
    ("obj_06_d", "06.d - Data Protection and Privacy", 2, "cat_6"),
    ("obj_09_aa", "09.aa - Audit Logging", 2, "cat_9"),
    ("obj_09_ab", "09.ab - Monitoring System Use", 2, "cat_9"),
    ("obj_09_m", "09.m - Network Controls", 2, "cat_9"),
    ("obj_10_a", "10.a - Security Requirements Analysis and Specification", 2, "cat_10"),
    ("obj_10_h", "10.h - Control of Technical Vulnerabilities", 2, "cat_10"),
    ("obj_11_a", "11.a - Reporting Information Security Events", 2, "cat_11"),
    ("obj_11_c", "11.c - Responsibilities and Procedures", 2, "cat_11"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_hitrust(conn) -> int:
    """Ingest HITRUST CSF regulatory taxonomy.

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
    for code, title, level, parent in REG_HITRUST_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_HITRUST_NODES:
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
        for code, title, level, parent in REG_HITRUST_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_HITRUST_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
