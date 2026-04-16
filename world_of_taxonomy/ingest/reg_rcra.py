"""RCRA regulatory taxonomy ingester.

Resource Conservation and Recovery Act (42 USC 6901 et seq.).
Authority: US Congress / EPA.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap82.htm

Data provenance: manual_transcription
License: Public Domain

Total: 29 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_rcra"
_SYSTEM_NAME = "RCRA"
_FULL_NAME = "Resource Conservation and Recovery Act (42 USC 6901 et seq.)"
_VERSION = "1976"
_REGION = "United States"
_AUTHORITY = "US Congress / EPA"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap82.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_RCRA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("sub_a", "Subtitle A - General Provisions", 1, None),
    ("sub_b", "Subtitle B - Office of Solid Waste and EPA Authorities", 1, None),
    ("sub_c", "Subtitle C - Hazardous Waste Management", 1, None),
    ("sub_d", "Subtitle D - State or Regional Solid Waste Plans", 1, None),
    ("sub_e", "Subtitle E - Duties of the Secretary of Commerce", 1, None),
    ("sub_f", "Subtitle F - Federal Responsibilities", 1, None),
    ("sub_g", "Subtitle G - Miscellaneous Provisions", 1, None),
    ("sub_h", "Subtitle H - Research and Development", 1, None),
    ("sub_i", "Subtitle I - Underground Storage Tanks", 1, None),
    ("sub_j", "Subtitle J - Medical Waste Tracking", 1, None),
    ("sec_3001", "Sec 3001 - Identification and Listing of Hazardous Waste", 2, "sub_c"),
    ("sec_3002", "Sec 3002 - Standards for Generators of Hazardous Waste", 2, "sub_c"),
    ("sec_3003", "Sec 3003 - Standards for Transporters of Hazardous Waste", 2, "sub_c"),
    ("sec_3004", "Sec 3004 - Standards for HW Treatment, Storage, and Disposal Facilities", 2, "sub_c"),
    ("sec_3005", "Sec 3005 - Permits for TSD Facilities", 2, "sub_c"),
    ("sec_3006", "Sec 3006 - Authorized State Programs", 2, "sub_c"),
    ("sec_3007", "Sec 3007 - Inspections", 2, "sub_c"),
    ("sec_3008", "Sec 3008 - Federal Enforcement", 2, "sub_c"),
    ("sec_3010", "Sec 3010 - Notification Requirements", 2, "sub_c"),
    ("sec_4001", "Sec 4001 - Objectives and Criteria for Solid Waste Disposal", 2, "sub_d"),
    ("sec_4004", "Sec 4004 - Criteria for Sanitary Landfills", 2, "sub_d"),
    ("sec_9001", "Sec 9001 - Definitions (UST Program)", 2, "sub_i"),
    ("sec_9003", "Sec 9003 - Release Detection, Prevention, and Correction", 2, "sub_i"),
    ("sec_9005", "Sec 9005 - Inspections, Monitoring, and Testing", 2, "sub_i"),
    ("cfr_260", "40 CFR Part 260 - Hazardous Waste Management System: General", 2, "sub_c"),
    ("cfr_261", "40 CFR Part 261 - Identification and Listing of Hazardous Waste", 2, "sub_c"),
    ("cfr_262", "40 CFR Part 262 - Standards for Generators", 2, "sub_c"),
    ("cfr_264", "40 CFR Part 264 - Standards for TSD Facility Owners and Operators", 2, "sub_c"),
    ("cfr_268", "40 CFR Part 268 - Land Disposal Restrictions", 2, "sub_c"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_rcra(conn) -> int:
    """Ingest RCRA regulatory taxonomy.

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
    for code, title, level, parent in REG_RCRA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_RCRA_NODES:
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
        for code, title, level, parent in REG_RCRA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_RCRA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
