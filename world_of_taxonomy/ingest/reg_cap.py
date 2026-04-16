"""CAP Accreditation regulatory taxonomy ingester.

College of American Pathologists Laboratory Accreditation Program.
Authority: College of American Pathologists (CAP).
Source: https://www.cap.org/laboratory-improvement/accreditation

Data provenance: manual_transcription
License: Proprietary (CAP)

Total: 21 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_cap"
_SYSTEM_NAME = "CAP Accreditation"
_FULL_NAME = "College of American Pathologists Laboratory Accreditation Program"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "College of American Pathologists (CAP)"
_SOURCE_URL = "https://www.cap.org/laboratory-improvement/accreditation"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (CAP)"

# (code, title, level, parent_code)
REG_CAP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("all_common", "ALL - All Common Checklist", 1, None),
    ("chem", "CHM - Chemistry and Toxicology", 1, None),
    ("hematology", "HEM - Hematology and Coagulation", 1, None),
    ("micro", "MIC - Microbiology", 1, None),
    ("immuno", "IMM - Immunology", 1, None),
    ("transfusion", "TRM - Transfusion Medicine", 1, None),
    ("molecular", "MOL - Molecular Pathology", 1, None),
    ("anatomic", "ANP - Anatomic Pathology", 1, None),
    ("cytology", "CYP - Cytopathology", 1, None),
    ("poc", "POC - Point-of-Care Testing", 1, None),
    ("cytogenetics", "CYG - Cytogenetics", 1, None),
    ("flow", "FLO - Flow Cytometry", 1, None),
    ("forensic", "FOR - Forensic Drug Testing", 1, None),
    ("all_quality", "Quality Management System", 2, "all_common"),
    ("all_personnel", "Personnel Requirements and Competency", 2, "all_common"),
    ("all_equipment", "Equipment, Instruments, and Reagents", 2, "all_common"),
    ("all_pre_analytic", "Pre-Analytic Processes", 2, "all_common"),
    ("all_analytic", "Analytic Processes", 2, "all_common"),
    ("all_post_analytic", "Post-Analytic Processes", 2, "all_common"),
    ("all_safety", "Laboratory Safety", 2, "all_common"),
    ("all_info_mgmt", "Information Management", 2, "all_common"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_cap(conn) -> int:
    """Ingest CAP Accreditation regulatory taxonomy.

    Returns 21 nodes.
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
    for code, title, level, parent in REG_CAP_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CAP_NODES:
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
        for code, title, level, parent in REG_CAP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CAP_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
