"""OSHA 29 CFR 1910 regulatory taxonomy ingester.

OSHA General Industry Standards (29 CFR Part 1910).
Authority: US Department of Labor / OSHA.
Source: https://www.ecfr.gov/current/title-29/subtitle-B/chapter-XVII/part-1910

Data provenance: manual_transcription
License: Public Domain

Total: 47 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_osha_1910"
_SYSTEM_NAME = "OSHA 29 CFR 1910"
_FULL_NAME = "OSHA General Industry Standards (29 CFR Part 1910)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Department of Labor / OSHA"
_SOURCE_URL = "https://www.ecfr.gov/current/title-29/subtitle-B/chapter-XVII/part-1910"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_OSHA_1910_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("sub_a", "Subpart A - General", 1, None),
    ("sub_b", "Subpart B - Adoption and Extension of Standards", 1, None),
    ("sub_d", "Subpart D - Walking-Working Surfaces", 1, None),
    ("sub_e", "Subpart E - Exit Routes and Emergency Planning", 1, None),
    ("sub_f", "Subpart F - Powered Platforms and Manlifts", 1, None),
    ("sub_g", "Subpart G - Occupational Health and Environmental Control", 1, None),
    ("sub_h", "Subpart H - Hazardous Materials", 1, None),
    ("sub_i", "Subpart I - Personal Protective Equipment", 1, None),
    ("sub_j", "Subpart J - General Environmental Controls", 1, None),
    ("sub_k", "Subpart K - Medical and First Aid", 1, None),
    ("sub_l", "Subpart L - Fire Protection", 1, None),
    ("sub_m", "Subpart M - Compressed Gas and Equipment", 1, None),
    ("sub_n", "Subpart N - Materials Handling and Storage", 1, None),
    ("sub_o", "Subpart O - Machinery and Machine Guarding", 1, None),
    ("sub_p", "Subpart P - Hand and Portable Powered Tools", 1, None),
    ("sub_q", "Subpart Q - Welding, Cutting and Brazing", 1, None),
    ("sub_r", "Subpart R - Special Industries", 1, None),
    ("sub_s", "Subpart S - Electrical", 1, None),
    ("sub_t", "Subpart T - Commercial Diving Operations", 1, None),
    ("sub_z", "Subpart Z - Toxic and Hazardous Substances", 1, None),
    ("s1910_23", "1910.23 - Ladders", 2, "sub_d"),
    ("s1910_25", "1910.25 - Stairways", 2, "sub_d"),
    ("s1910_27", "1910.27 - Scaffolds and Rope Descent Systems", 2, "sub_d"),
    ("s1910_36", "1910.36 - Design and Construction of Exit Routes", 2, "sub_e"),
    ("s1910_37", "1910.37 - Maintenance, Safeguards, and Operational Features", 2, "sub_e"),
    ("s1910_38", "1910.38 - Emergency Action Plans", 2, "sub_e"),
    ("s1910_39", "1910.39 - Fire Prevention Plans", 2, "sub_e"),
    ("s1910_95", "1910.95 - Occupational Noise Exposure", 2, "sub_g"),
    ("s1910_106", "1910.106 - Flammable Liquids", 2, "sub_h"),
    ("s1910_110", "1910.110 - Storage and Handling of Liquefied Petroleum Gases", 2, "sub_h"),
    ("s1910_119", "1910.119 - Process Safety Management of Highly Hazardous Chemicals", 2, "sub_h"),
    ("s1910_120", "1910.120 - Hazardous Waste Operations and Emergency Response", 2, "sub_h"),
    ("s1910_132", "1910.132 - General PPE Requirements", 2, "sub_i"),
    ("s1910_134", "1910.134 - Respiratory Protection", 2, "sub_i"),
    ("s1910_146", "1910.146 - Permit-Required Confined Spaces", 2, "sub_j"),
    ("s1910_147", "1910.147 - Control of Hazardous Energy (Lockout/Tagout)", 2, "sub_j"),
    ("s1910_151", "1910.151 - Medical Services and First Aid", 2, "sub_k"),
    ("s1910_157", "1910.157 - Portable Fire Extinguishers", 2, "sub_l"),
    ("s1910_178", "1910.178 - Powered Industrial Trucks", 2, "sub_n"),
    ("s1910_212", "1910.212 - General Requirements for All Machines", 2, "sub_o"),
    ("s1910_217", "1910.217 - Mechanical Power Presses", 2, "sub_o"),
    ("s1910_252", "1910.252 - General Welding Requirements", 2, "sub_q"),
    ("s1910_269", "1910.269 - Electric Power Generation, Transmission, and Distribution", 2, "sub_r"),
    ("s1910_303", "1910.303 - General Electrical Requirements", 2, "sub_s"),
    ("s1910_1001", "1910.1001 - Asbestos", 2, "sub_z"),
    ("s1910_1020", "1910.1020 - Access to Employee Exposure and Medical Records", 2, "sub_z"),
    ("s1910_1200", "1910.1200 - Hazard Communication", 2, "sub_z"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_osha_1910(conn) -> int:
    """Ingest OSHA 29 CFR 1910 regulatory taxonomy.

    Returns 47 nodes.
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
    for code, title, level, parent in REG_OSHA_1910_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_OSHA_1910_NODES:
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
        for code, title, level, parent in REG_OSHA_1910_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_OSHA_1910_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
