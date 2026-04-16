"""Clean Water Act regulatory taxonomy ingester.

Clean Water Act (Federal Water Pollution Control Act, 33 USC 1251 et seq.).
Authority: US Congress / EPA / Army Corps of Engineers.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title33/html/USCODE-2023-title33-chap26.htm

Data provenance: manual_transcription
License: Public Domain

Total: 26 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_clean_water"
_SYSTEM_NAME = "Clean Water Act"
_FULL_NAME = "Clean Water Act (Federal Water Pollution Control Act, 33 USC 1251 et seq.)"
_VERSION = "1972"
_REGION = "United States"
_AUTHORITY = "US Congress / EPA / Army Corps of Engineers"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title33/html/USCODE-2023-title33-chap26.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_CLEAN_WATER_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Research and Related Programs", 1, None),
    ("title_2", "Title II - Grants for Construction of Treatment Works", 1, None),
    ("title_3", "Title III - Standards and Enforcement", 1, None),
    ("title_4", "Title IV - Permits and Licenses", 1, None),
    ("title_5", "Title V - General Provisions", 1, None),
    ("title_6", "Title VI - State Water Pollution Control Revolving Funds", 1, None),
    ("sec_101", "Sec 101 - Objectives (Fishable/Swimmable Waters Goal)", 2, "title_1"),
    ("sec_104", "Sec 104 - Research, Investigations, and Training", 2, "title_1"),
    ("sec_201", "Sec 201 - Purpose of Construction Grants", 2, "title_2"),
    ("sec_301", "Sec 301 - Effluent Limitations", 2, "title_3"),
    ("sec_302", "Sec 302 - Water Quality Related Effluent Limitations", 2, "title_3"),
    ("sec_303", "Sec 303 - Water Quality Standards and TMDLs", 2, "title_3"),
    ("sec_304", "Sec 304 - Information and Guidelines", 2, "title_3"),
    ("sec_306", "Sec 306 - National Standards of Performance (New Sources)", 2, "title_3"),
    ("sec_307", "Sec 307 - Toxic and Pretreatment Standards", 2, "title_3"),
    ("sec_311", "Sec 311 - Oil and Hazardous Substance Liability (Spill Prevention)", 2, "title_3"),
    ("sec_316", "Sec 316 - Thermal Discharges and Cooling Water Intakes", 2, "title_3"),
    ("sec_319", "Sec 319 - Nonpoint Source Management Programs", 2, "title_3"),
    ("sec_401", "Sec 401 - Certification (State Water Quality Certification)", 2, "title_4"),
    ("sec_402", "Sec 402 - National Pollutant Discharge Elimination System (NPDES)", 2, "title_4"),
    ("sec_404", "Sec 404 - Permits for Dredged or Fill Material (Wetlands)", 2, "title_4"),
    ("sec_405", "Sec 405 - Disposal of Sewage Sludge (Biosolids)", 2, "title_4"),
    ("sec_501", "Sec 501 - Administration (General EPA Authority)", 2, "title_5"),
    ("sec_505", "Sec 505 - Citizen Suits", 2, "title_5"),
    ("sec_509", "Sec 509 - Judicial Review", 2, "title_5"),
    ("sec_601", "Sec 601 - State Water Pollution Control Revolving Fund (SRF)", 2, "title_6"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_clean_water(conn) -> int:
    """Ingest Clean Water Act regulatory taxonomy.

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
    for code, title, level, parent in REG_CLEAN_WATER_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CLEAN_WATER_NODES:
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
        for code, title, level, parent in REG_CLEAN_WATER_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CLEAN_WATER_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
