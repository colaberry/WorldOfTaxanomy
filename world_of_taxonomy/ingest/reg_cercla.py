"""CERCLA (Superfund) regulatory taxonomy ingester.

Comprehensive Environmental Response, Compensation, and Liability Act (42 USC 9601 et seq.).
Authority: US Congress / EPA.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap103.htm

Data provenance: manual_transcription
License: Public Domain

Total: 27 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_cercla"
_SYSTEM_NAME = "CERCLA (Superfund)"
_FULL_NAME = "Comprehensive Environmental Response, Compensation, and Liability Act (42 USC 9601 et seq.)"
_VERSION = "1980"
_REGION = "United States"
_AUTHORITY = "US Congress / EPA"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap103.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_CERCLA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Hazardous Substances Releases, Liability, Compensation", 1, None),
    ("title_2", "Title II - Hazardous Substance Response Revenue", 1, None),
    ("title_3", "Title III - Miscellaneous Provisions", 1, None),
    ("title_4", "Title IV - Pollution Insurance", 1, None),
    ("sec_101", "Sec 101 - Definitions", 2, "title_1"),
    ("sec_103", "Sec 103 - Notification Requirements", 2, "title_1"),
    ("sec_104", "Sec 104 - Response Authorities (Removal and Remedial Actions)", 2, "title_1"),
    ("sec_105", "Sec 105 - National Contingency Plan and National Priorities List", 2, "title_1"),
    ("sec_106", "Sec 106 - Abatement Actions (Unilateral Administrative Orders)", 2, "title_1"),
    ("sec_107", "Sec 107 - Liability (Strict, Joint and Several)", 2, "title_1"),
    ("sec_111", "Sec 111 - Uses of the Superfund", 2, "title_1"),
    ("sec_113", "Sec 113 - Settlements and Contribution", 2, "title_1"),
    ("sec_116", "Sec 116 - Schedules for Remedial Action", 2, "title_1"),
    ("sec_120", "Sec 120 - Federal Facilities", 2, "title_1"),
    ("sec_121", "Sec 121 - Cleanup Standards", 2, "title_1"),
    ("pa_si", "Preliminary Assessment / Site Inspection (PA/SI)", 2, "title_1"),
    ("npl_listing", "National Priorities List (NPL) Listing", 2, "title_1"),
    ("ri_fs", "Remedial Investigation / Feasibility Study (RI/FS)", 2, "title_1"),
    ("rod", "Record of Decision (ROD)", 2, "title_1"),
    ("rd_ra", "Remedial Design / Remedial Action (RD/RA)", 2, "title_1"),
    ("five_year", "Five-Year Reviews", 2, "title_1"),
    ("sec_211", "Sec 211 - Superfund Trust Fund", 2, "title_2"),
    ("sec_221", "Sec 221 - Tax on Certain Chemicals", 2, "title_2"),
    ("sec_302", "Sec 302 - Emergency Planning and Notification (EPCRA)", 2, "title_3"),
    ("sec_311", "Sec 311 - Material Safety Data Sheets (EPCRA)", 2, "title_3"),
    ("sec_312", "Sec 312 - Emergency and Hazardous Chemical Inventory (EPCRA)", 2, "title_3"),
    ("sec_313", "Sec 313 - Toxic Release Inventory (TRI) (EPCRA)", 2, "title_3"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_cercla(conn) -> int:
    """Ingest CERCLA (Superfund) regulatory taxonomy.

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
    for code, title, level, parent in REG_CERCLA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CERCLA_NODES:
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
        for code, title, level, parent in REG_CERCLA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CERCLA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
