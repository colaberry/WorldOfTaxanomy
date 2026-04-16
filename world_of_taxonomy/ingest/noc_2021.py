"""NOC 2021 (Canada National Occupational Classification 2021) ingester.

National Occupational Classification 2021 - Statistics Canada.
Classifies all occupations in the Canadian labour market into a 5-level
hierarchy. This ingester stores the top two levels: 10 broad categories
and ~43 major groups.

Source: Statistics Canada, Catalogue no. 12-583-X
License: Statistics Canada Open Licence

Crosswalk: major groups linked to ISCO-08 at the section level.
"""
from __future__ import annotations

MAJOR_GROUPS: list[tuple[str, str]] = [
    ("0", "Legislative and Senior Management Occupations"),
    ("1", "Business, Finance and Administration Occupations"),
    ("2", "Natural and Applied Sciences and Related Occupations"),
    ("3", "Health Occupations"),
    ("4", "Education, Law, Social, Community and Government Services"),
    ("5", "Arts, Culture, Recreation and Sport Occupations"),
    ("6", "Sales and Service Occupations"),
    ("7", "Trades, Transport and Equipment Operators and Related"),
    ("8", "Natural Resources, Agriculture and Related Production"),
    ("9", "Manufacturing and Utilities Occupations"),
]

SUB_MAJOR_GROUPS: list[tuple[str, str, str]] = [
    ("00", "Senior Management Occupations", "0"),
    ("01", "Middle Management - Retail, Wholesale and Customer Services", "0"),
    ("02", "Middle Management - Trades, Transportation, Production, Utilities", "0"),
    ("11", "Professional Occupations in Business and Finance", "1"),
    ("12", "Administrative and Financial Supervisors and Specialized Administrative Occupations", "1"),
    ("13", "Office Support Occupations", "1"),
    ("14", "Distribution, Tracking and Scheduling Coordination Occupations", "1"),
    ("21", "Professional Occupations in Natural and Applied Sciences", "2"),
    ("22", "Technical Occupations Related to Natural and Applied Sciences", "2"),
    ("30", "Professional Occupations in Health", "3"),
    ("31", "Technical Occupations in Health", "3"),
    ("32", "Assisting Occupations and Support Occupations in Health", "3"),
    ("40", "Professional Occupations in Education Services", "4"),
    ("41", "Professional Occupations in Law and Social, Community and Government Services", "4"),
    ("42", "Paraprofessional Occupations in Legal, Social, Community and Education Services", "4"),
    ("43", "Occupations in Front-Line Public Protection Services", "4"),
    ("44", "Care Providers and Educational, Legal and Public Protection Support Occupations", "4"),
    ("51", "Professional Occupations in Art and Culture", "5"),
    ("52", "Technical Occupations in Art, Culture, Recreation and Sport", "5"),
    ("53", "Sports and Recreation Occupations and Fitness Instructors", "5"),
    ("54", "Performers, Announcers and Other Performers", "5"),
    ("55", "Support Occupations in Art, Culture and Recreation", "5"),
    ("62", "Retail Sales Supervisors and Specialized Sales Occupations", "6"),
    ("63", "Service Supervisors and Specialized Service Occupations", "6"),
    ("64", "Sales Representatives and Salespersons - Wholesale and Technical", "6"),
    ("65", "Service Representatives and Other Customer and Personal Services", "6"),
    ("66", "Sales Support Occupations", "6"),
    ("67", "Service Support and Other Service Occupations NEC", "6"),
    ("70", "Technical Trades and Transportation Officers and Controllers", "7"),
    ("72", "Industrial, Electrical and Construction Trades", "7"),
    ("73", "Maintenance and Equipment Operation Trades", "7"),
    ("74", "Other Installers, Repairers and Material Handlers", "7"),
    ("75", "Transport and Heavy Equipment Operation and Related Maintenance", "7"),
    ("76", "Trades Helpers, Construction Labourers and Related Occupations", "7"),
    ("80", "Supervisors and Technical Occupations in Natural Resources and Agriculture", "8"),
    ("82", "Workers in Natural Resources, Agriculture and Related Production", "8"),
    ("84", "Harvesting, Landscaping and Natural Resources Labourers", "8"),
    ("90", "Processing, Manufacturing and Utilities Supervisors and Central Control Operators", "9"),
    ("92", "Processing and Manufacturing Machine Operators and Related Production Workers", "9"),
    ("93", "Assemblers in Manufacturing", "9"),
    ("95", "Labourers in Processing, Manufacturing and Utilities", "9"),
]

# Broad crosswalk from NOC major group to ISCO-08 major group
NOC_TO_ISCO: dict[str, str] = {
    "0": "1",
    "1": "4",
    "2": "2",
    "3": "2",
    "4": "2",
    "5": "3",
    "6": "5",
    "7": "7",
    "8": "6",
    "9": "8",
}


async def ingest_noc_2021(conn) -> int:
    """Ingest NOC 2021 into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "noc_2021",
        "NOC 2021",
        "National Occupational Classification 2021 (Statistics Canada)",
        "2021",
        "Canada",
        "Statistics Canada",
    )

    count = 0
    for seq, (code, title) in enumerate(MAJOR_GROUPS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "noc_2021", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(SUB_MAJOR_GROUPS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "noc_2021", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'noc_2021'",
        count,
    )

    # Crosswalk edges to ISCO-08 major groups
    for noc_code, isco_code in NOC_TO_ISCO.items():
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
                "noc_2021", noc_code, "isco_2008", isco_code, "broad",
                "NOC 2021 major group to ISCO-08 major group",
            )

    return count
