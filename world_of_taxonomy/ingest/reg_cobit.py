"""COBIT 2019 regulatory taxonomy ingester.

Control Objectives for Information and Related Technologies 2019.
Authority: ISACA.
Source: https://www.isaca.org/resources/cobit

Data provenance: manual_transcription
License: Proprietary (ISACA)

Total: 45 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_cobit"
_SYSTEM_NAME = "COBIT 2019"
_FULL_NAME = "Control Objectives for Information and Related Technologies 2019"
_VERSION = "2019"
_REGION = "Global"
_AUTHORITY = "ISACA"
_SOURCE_URL = "https://www.isaca.org/resources/cobit"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISACA)"

# (code, title, level, parent_code)
REG_COBIT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("edm", "EDM - Evaluate, Direct and Monitor", 1, None),
    ("apo", "APO - Align, Plan and Organize", 1, None),
    ("bai", "BAI - Build, Acquire and Implement", 1, None),
    ("dss", "DSS - Deliver, Service and Support", 1, None),
    ("mea", "MEA - Monitor, Evaluate and Assess", 1, None),
    ("edm01", "EDM01 - Ensured Governance Framework Setting and Maintenance", 2, "edm"),
    ("edm02", "EDM02 - Ensured Benefits Delivery", 2, "edm"),
    ("edm03", "EDM03 - Ensured Risk Optimization", 2, "edm"),
    ("edm04", "EDM04 - Ensured Resource Optimization", 2, "edm"),
    ("edm05", "EDM05 - Ensured Stakeholder Engagement", 2, "edm"),
    ("apo01", "APO01 - Managed I&T Management Framework", 2, "apo"),
    ("apo02", "APO02 - Managed Strategy", 2, "apo"),
    ("apo03", "APO03 - Managed Enterprise Architecture", 2, "apo"),
    ("apo04", "APO04 - Managed Innovation", 2, "apo"),
    ("apo05", "APO05 - Managed Portfolio", 2, "apo"),
    ("apo06", "APO06 - Managed Budget and Costs", 2, "apo"),
    ("apo07", "APO07 - Managed Human Resources", 2, "apo"),
    ("apo08", "APO08 - Managed Relationships", 2, "apo"),
    ("apo09", "APO09 - Managed Service Agreements", 2, "apo"),
    ("apo10", "APO10 - Managed Vendors", 2, "apo"),
    ("apo11", "APO11 - Managed Quality", 2, "apo"),
    ("apo12", "APO12 - Managed Risk", 2, "apo"),
    ("apo13", "APO13 - Managed Security", 2, "apo"),
    ("apo14", "APO14 - Managed Data", 2, "apo"),
    ("bai01", "BAI01 - Managed Programs", 2, "bai"),
    ("bai02", "BAI02 - Managed Requirements Definition", 2, "bai"),
    ("bai03", "BAI03 - Managed Solutions Identification and Build", 2, "bai"),
    ("bai04", "BAI04 - Managed Availability and Capacity", 2, "bai"),
    ("bai05", "BAI05 - Managed Organizational Change", 2, "bai"),
    ("bai06", "BAI06 - Managed IT Changes", 2, "bai"),
    ("bai07", "BAI07 - Managed IT Change Acceptance and Transitioning", 2, "bai"),
    ("bai08", "BAI08 - Managed Knowledge", 2, "bai"),
    ("bai09", "BAI09 - Managed Assets", 2, "bai"),
    ("bai10", "BAI10 - Managed Configuration", 2, "bai"),
    ("bai11", "BAI11 - Managed Projects", 2, "bai"),
    ("dss01", "DSS01 - Managed Operations", 2, "dss"),
    ("dss02", "DSS02 - Managed Service Requests and Incidents", 2, "dss"),
    ("dss03", "DSS03 - Managed Problems", 2, "dss"),
    ("dss04", "DSS04 - Managed Continuity", 2, "dss"),
    ("dss05", "DSS05 - Managed Security Services", 2, "dss"),
    ("dss06", "DSS06 - Managed Business Process Controls", 2, "dss"),
    ("mea01", "MEA01 - Managed Performance and Conformance Monitoring", 2, "mea"),
    ("mea02", "MEA02 - Managed System of Internal Control", 2, "mea"),
    ("mea03", "MEA03 - Managed Compliance with External Requirements", 2, "mea"),
    ("mea04", "MEA04 - Managed Assurance", 2, "mea"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_cobit(conn) -> int:
    """Ingest COBIT 2019 regulatory taxonomy.

    Returns 45 nodes.
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
    for code, title, level, parent in REG_COBIT_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_COBIT_NODES:
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
        for code, title, level, parent in REG_COBIT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_COBIT_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
