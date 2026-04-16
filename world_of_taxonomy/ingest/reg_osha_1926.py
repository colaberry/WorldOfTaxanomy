"""OSHA 29 CFR 1926 regulatory taxonomy ingester.

OSHA Construction Safety Standards (29 CFR Part 1926).
Authority: US Department of Labor / OSHA.
Source: https://www.ecfr.gov/current/title-29/subtitle-B/chapter-XVII/part-1926

Data provenance: manual_transcription
License: Public Domain

Total: 49 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_osha_1926"
_SYSTEM_NAME = "OSHA 29 CFR 1926"
_FULL_NAME = "OSHA Construction Safety Standards (29 CFR Part 1926)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Department of Labor / OSHA"
_SOURCE_URL = "https://www.ecfr.gov/current/title-29/subtitle-B/chapter-XVII/part-1926"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_OSHA_1926_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("sub_a", "Subpart A - General", 1, None),
    ("sub_b", "Subpart B - General Interpretations", 1, None),
    ("sub_c", "Subpart C - General Safety and Health Provisions", 1, None),
    ("sub_d", "Subpart D - Occupational Health and Environmental Controls", 1, None),
    ("sub_e", "Subpart E - Personal Protective and Life Saving Equipment", 1, None),
    ("sub_f", "Subpart F - Fire Protection and Prevention", 1, None),
    ("sub_g", "Subpart G - Signs, Signals, and Barricades", 1, None),
    ("sub_h", "Subpart H - Materials Handling, Storage, Use, and Disposal", 1, None),
    ("sub_i", "Subpart I - Tools (Hand and Power)", 1, None),
    ("sub_j", "Subpart J - Welding and Cutting", 1, None),
    ("sub_k", "Subpart K - Electrical", 1, None),
    ("sub_l", "Subpart L - Scaffolds", 1, None),
    ("sub_m", "Subpart M - Fall Protection", 1, None),
    ("sub_n", "Subpart N - Helicopters, Hoists, Elevators, and Conveyors", 1, None),
    ("sub_o", "Subpart O - Motor Vehicles and Mechanized Equipment", 1, None),
    ("sub_p", "Subpart P - Excavations", 1, None),
    ("sub_q", "Subpart Q - Concrete and Masonry Construction", 1, None),
    ("sub_r", "Subpart R - Steel Erection", 1, None),
    ("sub_s", "Subpart S - Underground Construction and Tunneling", 1, None),
    ("sub_t", "Subpart T - Demolition", 1, None),
    ("sub_u", "Subpart U - Blasting and Use of Explosives", 1, None),
    ("sub_v", "Subpart V - Power Transmission and Distribution", 1, None),
    ("sub_w", "Subpart W - Rollover Protective Structures", 1, None),
    ("sub_x", "Subpart X - Stairways and Ladders", 1, None),
    ("sub_z", "Subpart Z - Toxic and Hazardous Substances", 1, None),
    ("sub_aa", "Subpart AA - Confined Spaces in Construction", 1, None),
    ("sub_cc", "Subpart CC - Cranes and Derricks in Construction", 1, None),
    ("s1926_20", "1926.20 - General Safety and Health Provisions", 2, "sub_c"),
    ("s1926_21", "1926.21 - Safety Training and Education", 2, "sub_c"),
    ("s1926_32", "1926.32 - Definitions", 2, "sub_c"),
    ("s1926_50", "1926.50 - Medical Services and First Aid", 2, "sub_d"),
    ("s1926_55", "1926.55 - Gases, Vapors, Fumes, Dusts, and Mists", 2, "sub_d"),
    ("s1926_62", "1926.62 - Lead in Construction", 2, "sub_d"),
    ("s1926_100", "1926.100 - Head Protection", 2, "sub_e"),
    ("s1926_102", "1926.102 - Eye and Face Protection", 2, "sub_e"),
    ("s1926_150", "1926.150 - Fire Protection", 2, "sub_f"),
    ("s1926_200", "1926.200 - Accident Prevention Signs and Tags", 2, "sub_g"),
    ("s1926_251", "1926.251 - Rigging Equipment for Material Handling", 2, "sub_h"),
    ("s1926_451", "1926.451 - General Scaffold Requirements", 2, "sub_l"),
    ("s1926_501", "1926.501 - Fall Protection Duty to Have Fall Protection", 2, "sub_m"),
    ("s1926_502", "1926.502 - Fall Protection Systems Criteria", 2, "sub_m"),
    ("s1926_503", "1926.503 - Fall Protection Training", 2, "sub_m"),
    ("s1926_651", "1926.651 - Specific Excavation Requirements", 2, "sub_p"),
    ("s1926_652", "1926.652 - Protective Systems for Excavations", 2, "sub_p"),
    ("s1926_1101", "1926.1101 - Asbestos in Construction", 2, "sub_z"),
    ("s1926_1153", "1926.1153 - Respirable Crystalline Silica", 2, "sub_z"),
    ("s1926_1200", "1926.1200 - General Requirements for Confined Spaces", 2, "sub_aa"),
    ("s1926_1400", "1926.1400 - Scope of Cranes and Derricks", 2, "sub_cc"),
    ("s1926_1402", "1926.1402 - Ground Conditions", 2, "sub_cc"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_osha_1926(conn) -> int:
    """Ingest OSHA 29 CFR 1926 regulatory taxonomy.

    Returns 49 nodes.
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
    for code, title, level, parent in REG_OSHA_1926_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_OSHA_1926_NODES:
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
        for code, title, level, parent in REG_OSHA_1926_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_OSHA_1926_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
