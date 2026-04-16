"""ITAR regulatory taxonomy ingester.

International Traffic in Arms Regulations (22 CFR Parts 120-130).
Authority: US Department of State / Directorate of Defense Trade Controls (DDTC).
Source: https://www.ecfr.gov/current/title-22/chapter-I/subchapter-M

Data provenance: manual_transcription
License: Public Domain

Total: 32 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_itar"
_SYSTEM_NAME = "ITAR"
_FULL_NAME = "International Traffic in Arms Regulations (22 CFR Parts 120-130)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Department of State / Directorate of Defense Trade Controls (DDTC)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-22/chapter-I/subchapter-M"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_ITAR_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("part_120", "Part 120 - Purpose and Definitions", 1, None),
    ("part_121", "Part 121 - The United States Munitions List", 1, None),
    ("part_122", "Part 122 - Registration of Manufacturers and Exporters", 1, None),
    ("part_123", "Part 123 - Licenses for Defense Articles", 1, None),
    ("part_124", "Part 124 - Agreements, Off-Shore Procurement, Other", 1, None),
    ("part_125", "Part 125 - Licenses for Technical Data and Services", 1, None),
    ("part_126", "Part 126 - General Policies and Provisions", 1, None),
    ("part_127", "Part 127 - Violations and Penalties", 1, None),
    ("part_128", "Part 128 - Administrative Procedures", 1, None),
    ("part_129", "Part 129 - Brokering Activities", 1, None),
    ("part_130", "Part 130 - Political Contributions, Fees, and Commissions", 1, None),
    ("cat_i", "Category I - Firearms, Close Assault Weapons", 2, "part_121"),
    ("cat_ii", "Category II - Guns and Armament", 2, "part_121"),
    ("cat_iii", "Category III - Ammunition and Ordnance", 2, "part_121"),
    ("cat_iv", "Category IV - Launch Vehicles, Guided Missiles, Ballistic Missiles", 2, "part_121"),
    ("cat_v", "Category V - Explosives and Energetic Materials", 2, "part_121"),
    ("cat_vi", "Category VI - Surface Vessels of War and Special Naval Equipment", 2, "part_121"),
    ("cat_vii", "Category VII - Ground Vehicles", 2, "part_121"),
    ("cat_viii", "Category VIII - Aircraft and Related Articles", 2, "part_121"),
    ("cat_ix", "Category IX - Military Training Equipment and Training", 2, "part_121"),
    ("cat_x", "Category X - Personal Protective Equipment", 2, "part_121"),
    ("cat_xi", "Category XI - Military Electronics", 2, "part_121"),
    ("cat_xii", "Category XII - Fire Control, Laser, Imaging, and Guidance Equipment", 2, "part_121"),
    ("cat_xiii", "Category XIII - Materials and Miscellaneous Articles", 2, "part_121"),
    ("cat_xiv", "Category XIV - Toxicological Agents and Equipment", 2, "part_121"),
    ("cat_xv", "Category XV - Spacecraft and Related Articles", 2, "part_121"),
    ("cat_xvi", "Category XVI - Nuclear Weapons Related Articles", 2, "part_121"),
    ("cat_xvii", "Category XVII - Classified Articles, Technical Data, and Defense Services", 2, "part_121"),
    ("cat_xviii", "Category XVIII - Directed Energy Weapons", 2, "part_121"),
    ("cat_xix", "Category XIX - Gas Turbine Engines and Associated Equipment", 2, "part_121"),
    ("cat_xx", "Category XX - Submersible Vessels and Related Articles", 2, "part_121"),
    ("cat_xxi", "Category XXI - Articles, Technical Data, and Defense Services Not Otherwise Enumerated", 2, "part_121"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_itar(conn) -> int:
    """Ingest ITAR regulatory taxonomy.

    Returns 32 nodes.
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
    for code, title, level, parent in REG_ITAR_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_ITAR_NODES:
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
        for code, title, level, parent in REG_ITAR_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_ITAR_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
