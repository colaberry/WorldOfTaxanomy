"""ASHRAE Standards regulatory taxonomy ingester.

ASHRAE Key Standards for HVAC, Refrigeration, and Building Performance.
Authority: American Society of Heating, Refrigerating and Air-Conditioning Engineers.
Source: https://www.ashrae.org/technical-resources/standards-and-guidelines

Data provenance: manual_transcription
License: Proprietary (ASHRAE)

Total: 23 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_ashrae"
_SYSTEM_NAME = "ASHRAE Standards"
_FULL_NAME = "ASHRAE Key Standards for HVAC, Refrigeration, and Building Performance"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "American Society of Heating, Refrigerating and Air-Conditioning Engineers"
_SOURCE_URL = "https://www.ashrae.org/technical-resources/standards-and-guidelines"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ASHRAE)"

# (code, title, level, parent_code)
REG_ASHRAE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("energy", "Energy Standards", 1, None),
    ("iaq", "Indoor Air Quality Standards", 1, None),
    ("hvac", "HVAC Design Standards", 1, None),
    ("refrigeration", "Refrigeration Standards", 1, None),
    ("commissioning", "Commissioning Standards", 1, None),
    ("sustainability", "Sustainability Standards", 1, None),
    ("std_90_1", "Standard 90.1 - Energy Standard for Buildings Except Low-Rise Residential", 2, "energy"),
    ("std_90_2", "Standard 90.2 - Energy-Efficient Design of Low-Rise Residential Buildings", 2, "energy"),
    ("std_100", "Standard 100 - Energy Efficiency in Existing Buildings", 2, "energy"),
    ("std_105", "Standard 105 - Standard Methods of Determining, Expressing, and Comparing Building Energy Performance", 2, "energy"),
    ("std_62_1", "Standard 62.1 - Ventilation for Acceptable Indoor Air Quality", 2, "iaq"),
    ("std_62_2", "Standard 62.2 - Ventilation and Acceptable IAQ in Residential Buildings", 2, "iaq"),
    ("std_170", "Standard 170 - Ventilation of Health Care Facilities", 2, "iaq"),
    ("std_15", "Standard 15 - Safety Standard for Refrigeration Systems", 2, "hvac"),
    ("std_34", "Standard 34 - Designation and Safety Classification of Refrigerants", 2, "hvac"),
    ("std_55", "Standard 55 - Thermal Environmental Conditions for Human Occupancy", 2, "hvac"),
    ("std_15_ref", "Standard 15 - Safety Standard for Refrigeration Systems and Nomenclature", 2, "refrigeration"),
    ("std_34_ref", "Standard 34 - Refrigerant Safety Classification", 2, "refrigeration"),
    ("std_202", "Standard 202 - Commissioning Process for Buildings and Systems", 2, "commissioning"),
    ("guideline_0", "Guideline 0 - The Commissioning Process", 2, "commissioning"),
    ("guideline_1_1", "Guideline 1.1 - HVAC&R Technical Requirements for the Commissioning Process", 2, "commissioning"),
    ("std_189_1", "Standard 189.1 - Standard for the Design of High-Performance Green Buildings", 2, "sustainability"),
    ("std_228", "Standard 228 - Standard Method of Evaluating Zero Net Energy Building Performance", 2, "sustainability"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_ashrae(conn) -> int:
    """Ingest ASHRAE Standards regulatory taxonomy.

    Returns 23 nodes.
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
    for code, title, level, parent in REG_ASHRAE_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_ASHRAE_NODES:
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
        for code, title, level, parent in REG_ASHRAE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_ASHRAE_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
