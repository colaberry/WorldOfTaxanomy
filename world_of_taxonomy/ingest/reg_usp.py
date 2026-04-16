"""USP Chapters regulatory taxonomy ingester.

United States Pharmacopeia Key General Chapters.
Authority: United States Pharmacopeia (USP).
Source: https://www.usp.org/

Data provenance: manual_transcription
License: Proprietary (USP)

Total: 21 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_usp"
_SYSTEM_NAME = "USP Chapters"
_FULL_NAME = "United States Pharmacopeia Key General Chapters"
_VERSION = "USP-NF 2024"
_REGION = "United States"
_AUTHORITY = "United States Pharmacopeia (USP)"
_SOURCE_URL = "https://www.usp.org/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (USP)"

# (code, title, level, parent_code)
REG_USP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("general_tests", "General Tests and Assays (<1>-<999>)", 1, None),
    ("general_info", "General Information Chapters (<1000>-<1999>)", 1, None),
    ("dietary", "Dietary Supplements (<2000>-<2999>)", 1, None),
    ("usp_1", "<1> Injections and Implanted Drug Products", 2, "general_tests"),
    ("usp_61", "<61> Microbiological Examination of Nonsterile Products", 2, "general_tests"),
    ("usp_62", "<62> Microbiological Examination of Nonsterile Products: Tests for Specified Microorganisms", 2, "general_tests"),
    ("usp_71", "<71> Sterility Tests", 2, "general_tests"),
    ("usp_85", "<85> Bacterial Endotoxins Test", 2, "general_tests"),
    ("usp_232", "<232> Elemental Impurities - Limits", 2, "general_tests"),
    ("usp_233", "<233> Elemental Impurities - Procedures", 2, "general_tests"),
    ("usp_467", "<467> Residual Solvents", 2, "general_tests"),
    ("usp_621", "<621> Chromatography", 2, "general_tests"),
    ("usp_731", "<731> Loss on Drying", 2, "general_tests"),
    ("usp_905", "<905> Uniformity of Dosage Units", 2, "general_tests"),
    ("usp_921", "<921> Water Determination", 2, "general_tests"),
    ("usp_1058", "<1058> Analytical Instrument Qualification", 2, "general_info"),
    ("usp_1072", "<1072> Disinfectants and Antiseptics", 2, "general_info"),
    ("usp_1116", "<1116> Microbiological Control and Monitoring of Aseptic Processing Environments", 2, "general_info"),
    ("usp_1211", "<1211> Sterilization and Sterility Assurance", 2, "general_info"),
    ("usp_1225", "<1225> Validation of Compendial Procedures", 2, "general_info"),
    ("usp_1226", "<1226> Verification of Compendial Procedures", 2, "general_info"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_usp(conn) -> int:
    """Ingest USP Chapters regulatory taxonomy.

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
    for code, title, level, parent in REG_USP_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_USP_NODES:
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
        for code, title, level, parent in REG_USP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_USP_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
