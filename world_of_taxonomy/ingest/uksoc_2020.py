"""UK SOC 2020 (United Kingdom Standard Occupational Classification 2020) ingester.

Standard Occupational Classification 2020 - Office for National Statistics (UK).
Four-level hierarchy: major groups (1 digit), sub-major groups (2 digit),
minor groups (3 digit), unit groups (4 digit). This ingester covers the
top two levels: 9 major groups and 34 sub-major groups.

Source: Office for National Statistics, Volume 1 - Structure and descriptions
License: Open Government Licence v3.0

Crosswalk: major groups linked to ISCO-08.
"""
from __future__ import annotations

MAJOR_GROUPS: list[tuple[str, str]] = [
    ("1", "Managers, Directors and Senior Officials"),
    ("2", "Professional Occupations"),
    ("3", "Associate Professional and Technical Occupations"),
    ("4", "Administrative and Secretarial Occupations"),
    ("5", "Skilled Trades Occupations"),
    ("6", "Caring, Leisure and Other Service Occupations"),
    ("7", "Sales and Customer Service Occupations"),
    ("8", "Process, Plant and Machine Operatives"),
    ("9", "Elementary Occupations"),
]

SUB_MAJOR_GROUPS: list[tuple[str, str, str]] = [
    ("11", "Corporate Managers and Directors", "1"),
    ("12", "Other Managers and Proprietors", "1"),
    ("21", "Science, Research, Engineering and Technology Professionals", "2"),
    ("22", "Health Professionals", "2"),
    ("23", "Teaching and Educational Professionals", "2"),
    ("24", "Business, Media and Public Service Professionals", "2"),
    ("25", "IT and Telecommunications Professionals", "2"),
    ("26", "Legal, Social and Welfare Professionals", "2"),
    ("31", "Science, Engineering and Technology Associate Professionals", "3"),
    ("32", "Health and Social Care Associate Professionals", "3"),
    ("33", "Protective Service Occupations", "3"),
    ("34", "Culture, Media and Sports Occupations", "3"),
    ("35", "Business and Public Service Associate Professionals", "3"),
    ("41", "Administrative Occupations: Government and Related Organisations", "4"),
    ("42", "Administrative Occupations: Commerce", "4"),
    ("43", "Secretarial and Related Occupations", "4"),
    ("51", "Agricultural and Related Trades", "5"),
    ("52", "Metal Forming, Welding and Related Trades", "5"),
    ("53", "Metal Machining, Fitting and Instrument Making Trades", "5"),
    ("54", "Vehicle Trades", "5"),
    ("55", "Electrical and Electronic Trades", "5"),
    ("56", "Construction and Building Trades", "5"),
    ("57", "Building Completion Trades", "5"),
    ("58", "Textiles, Printing and Other Skilled Trades", "5"),
    ("61", "Caring Personal Service Occupations", "6"),
    ("62", "Leisure, Travel and Related Personal Service Occupations", "6"),
    ("63", "Animal Care and Related Services", "6"),
    ("71", "Sales Occupations", "7"),
    ("72", "Customer Service Occupations", "7"),
    ("81", "Process, Plant and Machine Operatives", "8"),
    ("82", "Transport Drivers and Operatives", "8"),
    ("83", "Mobile Machine Drivers and Operatives", "8"),
    ("91", "Elementary Trades and Related Occupations", "9"),
    ("92", "Elementary Administration and Service Occupations", "9"),
]

# Broad crosswalk from UK SOC major group to ISCO-08 major group
UKSOC_TO_ISCO: dict[str, str] = {
    "1": "1",
    "2": "2",
    "3": "3",
    "4": "4",
    "5": "7",
    "6": "5",
    "7": "5",
    "8": "8",
    "9": "9",
}


async def ingest_uksoc_2020(conn) -> int:
    """Ingest UK SOC 2020 into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "uksoc_2020",
        "UK SOC 2020",
        "United Kingdom Standard Occupational Classification 2020",
        "2020",
        "United Kingdom",
        "Office for National Statistics (ONS)",
    )

    count = 0
    for seq, (code, title) in enumerate(MAJOR_GROUPS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "uksoc_2020", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(SUB_MAJOR_GROUPS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "uksoc_2020", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'uksoc_2020'",
        count,
    )

    for soc_code, isco_code in UKSOC_TO_ISCO.items():
        isco_exists = await conn.fetchval(
            "SELECT 1 FROM classification_node WHERE system_id = 'isco_2008' AND code = $1",
            isco_code,
        )
        if isco_exists:
            await conn.execute(
                """INSERT INTO equivalence
                       (source_system, source_code, target_system, target_code, match_type, notes)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
                "uksoc_2020", soc_code, "isco_2008", isco_code, "broad",
                "UK SOC 2020 major group to ISCO-08 major group",
            )

    return count
