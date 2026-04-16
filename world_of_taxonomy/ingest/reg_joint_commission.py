"""Joint Commission Standards regulatory taxonomy ingester.

The Joint Commission Accreditation Standards for Hospitals.
Authority: The Joint Commission (TJC).
Source: https://www.jointcommission.org/standards/

Data provenance: manual_transcription
License: Proprietary (TJC)

Total: 30 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_joint_commission"
_SYSTEM_NAME = "Joint Commission Standards"
_FULL_NAME = "The Joint Commission Accreditation Standards for Hospitals"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "The Joint Commission (TJC)"
_SOURCE_URL = "https://www.jointcommission.org/standards/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (TJC)"

# (code, title, level, parent_code)
REG_JOINT_COMMISSION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ec", "EC - Environment of Care", 1, None),
    ("em", "EM - Emergency Management", 1, None),
    ("hr", "HR - Human Resources", 1, None),
    ("ic", "IC - Infection Prevention and Control", 1, None),
    ("im", "IM - Information Management", 1, None),
    ("ld", "LD - Leadership", 1, None),
    ("ls", "LS - Life Safety", 1, None),
    ("mm", "MM - Medication Management", 1, None),
    ("ms", "MS - Medical Staff", 1, None),
    ("npsg", "NPSG - National Patient Safety Goals", 1, None),
    ("pc", "PC - Provision of Care, Treatment, and Services", 1, None),
    ("pi", "PI - Performance Improvement", 1, None),
    ("rc", "RC - Record of Care, Treatment, and Services", 1, None),
    ("ri", "RI - Rights and Responsibilities of the Individual", 1, None),
    ("ts", "TS - Transplant Safety", 1, None),
    ("waived", "WT - Waived Testing", 1, None),
    ("npsg_01", "NPSG.01 - Improve the accuracy of patient identification", 2, "npsg"),
    ("npsg_02", "NPSG.02 - Improve the effectiveness of communication", 2, "npsg"),
    ("npsg_03", "NPSG.03 - Improve the safety of using medications", 2, "npsg"),
    ("npsg_06", "NPSG.06 - Reduce the harm associated with clinical alarm systems", 2, "npsg"),
    ("npsg_07", "NPSG.07 - Reduce the risk of health care-associated infections", 2, "npsg"),
    ("npsg_15", "NPSG.15 - Identify safety risks inherent in the patient population (suicide)", 2, "npsg"),
    ("npsg_16", "NPSG.16 - Improve the safety of clinical alarm systems", 2, "npsg"),
    ("ec_01", "EC.01 - Safety and Security Management Plans", 2, "ec"),
    ("ec_02", "EC.02 - Hazardous Materials and Waste Management", 2, "ec"),
    ("ld_01", "LD.01 - Governance and Leadership Structure", 2, "ld"),
    ("mm_01", "MM.01 - Medication Procurement and Storage", 2, "mm"),
    ("mm_03", "MM.03 - Medication Administration", 2, "mm"),
    ("pc_01", "PC.01 - Assessment of Patients", 2, "pc"),
    ("pc_02", "PC.02 - Planning Care", 2, "pc"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_joint_commission(conn) -> int:
    """Ingest Joint Commission Standards regulatory taxonomy.

    Returns 30 nodes.
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
    for code, title, level, parent in REG_JOINT_COMMISSION_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_JOINT_COMMISSION_NODES:
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
        for code, title, level, parent in REG_JOINT_COMMISSION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_JOINT_COMMISSION_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
