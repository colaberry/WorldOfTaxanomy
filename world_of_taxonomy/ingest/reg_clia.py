"""CLIA Regulations regulatory taxonomy ingester.

Clinical Laboratory Improvement Amendments of 1988 (42 CFR Part 493).
Authority: US Congress / CMS.
Source: https://www.ecfr.gov/current/title-42/chapter-IV/subchapter-G/part-493

Data provenance: manual_transcription
License: Public Domain

Total: 20 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_clia"
_SYSTEM_NAME = "CLIA Regulations"
_FULL_NAME = "Clinical Laboratory Improvement Amendments of 1988 (42 CFR Part 493)"
_VERSION = "1988"
_REGION = "United States"
_AUTHORITY = "US Congress / CMS"
_SOURCE_URL = "https://www.ecfr.gov/current/title-42/chapter-IV/subchapter-G/part-493"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_CLIA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("sub_a", "Subpart A - General Provisions", 1, None),
    ("sub_b", "Subpart B - Certificate of Waiver", 1, None),
    ("sub_c", "Subpart C - Registration Certificate and Certificate for PPM Procedures", 1, None),
    ("sub_d", "Subpart D - Certificate of Accreditation", 1, None),
    ("sub_e", "Subpart E - Accreditation by a Private, Nonprofit Organization", 1, None),
    ("sub_h", "Subpart H - Participation in Proficiency Testing", 1, None),
    ("sub_j", "Subpart J - Patient Test Management for Moderate and High Complexity Testing", 1, None),
    ("sub_k", "Subpart K - Quality Control for Tests of Moderate Complexity", 1, None),
    ("sub_m", "Subpart M - Personnel for Moderate and High Complexity Testing", 1, None),
    ("sub_q", "Subpart Q - Inspection", 1, None),
    ("sub_r", "Subpart R - Enforcement Procedures", 1, None),
    ("sec_493_3", "493.3 - Applicability", 2, "sub_a"),
    ("sec_493_15", "493.15 - Testing Categories (Waived, Moderate, High)", 2, "sub_a"),
    ("sec_493_801", "493.801 - Proficiency Testing Enrollment and Testing", 2, "sub_h"),
    ("sec_493_1100", "493.1100 - Condition: Specimen Collection and Test Requisition", 2, "sub_j"),
    ("sec_493_1200", "493.1200 - Standard: Establishment and Verification of Performance Specifications", 2, "sub_k"),
    ("sec_493_1250", "493.1250 - Standard: General Quality Control Procedures", 2, "sub_k"),
    ("sec_493_1443", "493.1443 - Standard: Laboratory Director Responsibilities", 2, "sub_m"),
    ("sec_493_1451", "493.1451 - Standard: Technical Consultant Responsibilities", 2, "sub_m"),
    ("sec_493_1800", "493.1800 - Inspection Basis and Frequency", 2, "sub_q"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_clia(conn) -> int:
    """Ingest CLIA Regulations regulatory taxonomy.

    Returns 20 nodes.
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
    for code, title, level, parent in REG_CLIA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CLIA_NODES:
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
        for code, title, level, parent in REG_CLIA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CLIA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
