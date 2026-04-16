"""EAR regulatory taxonomy ingester.

Export Administration Regulations (15 CFR Parts 730-774).
Authority: US Department of Commerce / Bureau of Industry and Security (BIS).
Source: https://www.ecfr.gov/current/title-15/subtitle-B/chapter-VII/subchapter-C

Data provenance: manual_transcription
License: Public Domain

Total: 31 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_ear"
_SYSTEM_NAME = "EAR"
_FULL_NAME = "Export Administration Regulations (15 CFR Parts 730-774)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Department of Commerce / Bureau of Industry and Security (BIS)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-15/subtitle-B/chapter-VII/subchapter-C"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_EAR_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("part_730", "Part 730 - General Information", 1, None),
    ("part_732", "Part 732 - Steps for Using the EAR", 1, None),
    ("part_734", "Part 734 - Scope of the EAR", 1, None),
    ("part_736", "Part 736 - General Prohibitions", 1, None),
    ("part_738", "Part 738 - Commerce Control List Overview", 1, None),
    ("part_740", "Part 740 - License Exceptions", 1, None),
    ("part_742", "Part 742 - Control Policy (CCL Based Controls)", 1, None),
    ("part_744", "Part 744 - Control Policy: End-User and End-Use Based", 1, None),
    ("part_746", "Part 746 - Embargoes and Other Special Controls", 1, None),
    ("part_748", "Part 748 - Applications, Classification, Advisory Opinions", 1, None),
    ("part_750", "Part 750 - Application Processing, Issuance, and Denial", 1, None),
    ("part_754", "Part 754 - Short Supply Controls", 1, None),
    ("part_758", "Part 758 - Export Clearance Requirements", 1, None),
    ("part_760", "Part 760 - Restrictive Trade Practices or Boycotts", 1, None),
    ("part_762", "Part 762 - Recordkeeping", 1, None),
    ("part_764", "Part 764 - Enforcement and Protective Measures", 1, None),
    ("part_766", "Part 766 - Administrative Enforcement Proceedings", 1, None),
    ("part_768", "Part 768 - Foreign Availability Determination", 1, None),
    ("part_770", "Part 770 - Interpretations", 1, None),
    ("part_772", "Part 772 - Definitions of Terms", 1, None),
    ("part_774", "Part 774 - Commerce Control List", 1, None),
    ("ccl_0", "Category 0 - Nuclear Materials, Facilities, and Equipment", 2, "part_774"),
    ("ccl_1", "Category 1 - Special Materials and Related Equipment", 2, "part_774"),
    ("ccl_2", "Category 2 - Materials Processing", 2, "part_774"),
    ("ccl_3", "Category 3 - Electronics", 2, "part_774"),
    ("ccl_4", "Category 4 - Computers", 2, "part_774"),
    ("ccl_5", "Category 5 - Telecommunications and Information Security", 2, "part_774"),
    ("ccl_6", "Category 6 - Sensors and Lasers", 2, "part_774"),
    ("ccl_7", "Category 7 - Navigation and Avionics", 2, "part_774"),
    ("ccl_8", "Category 8 - Marine", 2, "part_774"),
    ("ccl_9", "Category 9 - Aerospace and Propulsion", 2, "part_774"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_ear(conn) -> int:
    """Ingest EAR regulatory taxonomy.

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
    for code, title, level, parent in REG_EAR_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_EAR_NODES:
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
        for code, title, level, parent in REG_EAR_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_EAR_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
