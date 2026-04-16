"""ANSI/ASME Standards regulatory taxonomy ingester.

Key ASME Standards for Mechanical Engineering and Pressure Equipment.
Authority: American Society of Mechanical Engineers (ASME).
Source: https://www.asme.org/codes-standards

Data provenance: manual_transcription
License: Proprietary (ASME)

Total: 26 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_asme"
_SYSTEM_NAME = "ANSI/ASME Standards"
_FULL_NAME = "Key ASME Standards for Mechanical Engineering and Pressure Equipment"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "American Society of Mechanical Engineers (ASME)"
_SOURCE_URL = "https://www.asme.org/codes-standards"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ASME)"

# (code, title, level, parent_code)
REG_ASME_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("boiler", "Boiler and Pressure Vessel Code (BPVC)", 1, None),
    ("piping", "Piping and Pipeline Standards", 1, None),
    ("elevators", "Elevator and Escalator Standards", 1, None),
    ("cranes", "Crane and Hoist Standards", 1, None),
    ("dimensioning", "Dimensioning and Tolerancing", 1, None),
    ("nuclear", "Nuclear Standards", 1, None),
    ("bpvc_i", "BPVC Section I - Rules for Construction of Power Boilers", 2, "boiler"),
    ("bpvc_ii", "BPVC Section II - Materials", 2, "boiler"),
    ("bpvc_iv", "BPVC Section IV - Rules for Construction of Heating Boilers", 2, "boiler"),
    ("bpvc_v", "BPVC Section V - Nondestructive Examination", 2, "boiler"),
    ("bpvc_viii_1", "BPVC Section VIII Div 1 - Rules for Construction of Pressure Vessels", 2, "boiler"),
    ("bpvc_viii_2", "BPVC Section VIII Div 2 - Alternative Rules (Pressure Vessels)", 2, "boiler"),
    ("bpvc_ix", "BPVC Section IX - Qualification Standard for Welding", 2, "boiler"),
    ("bpvc_x", "BPVC Section X - Fiber-Reinforced Plastic Pressure Vessels", 2, "boiler"),
    ("bpvc_xi", "BPVC Section XI - Rules for In-service Inspection (Nuclear)", 2, "boiler"),
    ("bpvc_xii", "BPVC Section XII - Transport Tanks", 2, "boiler"),
    ("b31_1", "B31.1 - Power Piping", 2, "piping"),
    ("b31_3", "B31.3 - Process Piping", 2, "piping"),
    ("b31_4", "B31.4 - Pipeline Transportation Systems for Liquids", 2, "piping"),
    ("b31_8", "B31.8 - Gas Transmission and Distribution Piping", 2, "piping"),
    ("a17_1", "A17.1/CSA B44 - Safety Code for Elevators and Escalators", 2, "elevators"),
    ("a17_2", "A17.2 - Guide for Inspection of Elevators and Escalators", 2, "elevators"),
    ("b30_series", "B30 Series - Safety Standards for Cableways, Cranes, Derricks, and Hoists", 2, "cranes"),
    ("y14_5", "Y14.5 - Dimensioning and Tolerancing (GD&T)", 2, "dimensioning"),
    ("y14_100", "Y14.100 - Engineering Drawing Practices", 2, "dimensioning"),
    ("nqa_1", "NQA-1 - Quality Assurance Requirements for Nuclear Facility Applications", 2, "nuclear"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_asme(conn) -> int:
    """Ingest ANSI/ASME Standards regulatory taxonomy.

    Returns 26 nodes.
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
    for code, title, level, parent in REG_ASME_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_ASME_NODES:
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
        for code, title, level, parent in REG_ASME_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_ASME_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
