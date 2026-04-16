"""ADA regulatory taxonomy ingester.

Americans with Disabilities Act of 1990 (42 USC 12101 et seq.).
Authority: US Congress / DOJ / EEOC.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap126.htm

Data provenance: manual_transcription
License: Public Domain

Total: 31 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_ada"
_SYSTEM_NAME = "ADA"
_FULL_NAME = "Americans with Disabilities Act of 1990 (42 USC 12101 et seq.)"
_VERSION = "1990"
_REGION = "United States"
_AUTHORITY = "US Congress / DOJ / EEOC"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap126.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_ADA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Employment", 1, None),
    ("title_2", "Title II - Public Services (State and Local Government)", 1, None),
    ("title_3", "Title III - Public Accommodations and Commercial Facilities", 1, None),
    ("title_4", "Title IV - Telecommunications", 1, None),
    ("title_5", "Title V - Miscellaneous Provisions", 1, None),
    ("t1_sec101", "Sec 101 - Definitions (Disability, Qualified Individual)", 2, "title_1"),
    ("t1_sec102", "Sec 102 - Discrimination Prohibited", 2, "title_1"),
    ("t1_sec103", "Sec 103 - Defenses (Undue Hardship, Direct Threat)", 2, "title_1"),
    ("t1_sec104", "Sec 104 - Illegal Use of Drugs and Alcohol", 2, "title_1"),
    ("t1_sec105", "Sec 105 - Posting Notices", 2, "title_1"),
    ("t1_sec106", "Sec 106 - Regulations (EEOC)", 2, "title_1"),
    ("t1_sec107", "Sec 107 - Enforcement (Remedies and Procedures)", 2, "title_1"),
    ("t1_reasonable", "Reasonable Accommodation Requirements", 2, "title_1"),
    ("t2_sub_a", "Subtitle A - General Prohibitions Against Discrimination", 2, "title_2"),
    ("t2_sub_b", "Subtitle B - Public Transportation (Non-Rail)", 2, "title_2"),
    ("t2_sec201", "Sec 201 - Definition of Qualified Individual", 2, "title_2"),
    ("t2_sec202", "Sec 202 - Discrimination Prohibited", 2, "title_2"),
    ("t2_sec203", "Sec 203 - Enforcement", 2, "title_2"),
    ("t2_sec204", "Sec 204 - Regulations (DOJ)", 2, "title_2"),
    ("t3_sec301", "Sec 301 - Definitions (Public Accommodation, Commercial Facility)", 2, "title_3"),
    ("t3_sec302", "Sec 302 - Prohibition of Discrimination by Public Accommodations", 2, "title_3"),
    ("t3_sec303", "Sec 303 - New Construction and Alterations", 2, "title_3"),
    ("t3_sec304", "Sec 304 - Transportation Services by Private Entities", 2, "title_3"),
    ("t3_sec308", "Sec 308 - Enforcement (Private Actions, AG Actions)", 2, "title_3"),
    ("t4_sec401", "Sec 401 - Telecommunications Relay Services", 2, "title_4"),
    ("t4_sec402", "Sec 402 - Closed Captioning", 2, "title_4"),
    ("t5_sec501", "Sec 501 - Retaliation and Coercion Prohibited", 2, "title_5"),
    ("t5_sec503", "Sec 503 - State Immunity Not Waived", 2, "title_5"),
    ("t5_sec507", "Sec 507 - Relationship to Other Laws", 2, "title_5"),
    ("t5_sec508", "Sec 508 - Transvestism Not Covered", 2, "title_5"),
    ("t5_adaaa", "ADA Amendments Act of 2008 (ADAAA) Conforming Changes", 2, "title_5"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_ada(conn) -> int:
    """Ingest ADA regulatory taxonomy.

    Returns 31 nodes.
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
    for code, title, level, parent in REG_ADA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_ADA_NODES:
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
        for code, title, level, parent in REG_ADA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_ADA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
