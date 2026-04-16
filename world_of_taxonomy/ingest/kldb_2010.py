"""KldB 2010 (Klassifikation der Berufe 2010) ingester.

German Classification of Occupations 2010 - Bundesagentur fuer Arbeit.
Five-level hierarchy. This ingester stores the top two levels:
10 occupational areas (Berufsbereiche) and ~44 main occupational groups
(Berufshauptgruppen).

Source: Bundesagentur fuer Arbeit, Klassifikation der Berufe 2010
License: Datenlizenz Deutschland - Namensnennung - Version 2.0

Crosswalk: occupational areas linked to ISCO-08 major groups.
"""
from __future__ import annotations

AREAS: list[tuple[str, str]] = [
    ("1", "Agriculture, Forestry, Horticulture and Fishing"),
    ("2", "Raw Materials Extraction, Glass, Ceramics and Construction Materials"),
    ("3", "Chemical, Pharmaceutical, Plastic and Wood Products"),
    ("4", "Paper, Printing, Wood, Furniture and Other Manufacturing"),
    ("5", "Metal Production, Metalworking, Machine and Transport Technology"),
    ("6", "Electrical Technology, Electronics and Computer Technology"),
    ("7", "Technical Services, IT and Natural Sciences"),
    ("8", "Construction and Technical Planning"),
    ("9", "Trade, Commerce and Services"),
    ("0", "Military and Civil Defence Occupations"),
]

MAIN_GROUPS: list[tuple[str, str, str]] = [
    ("11", "Farmers, Horticulturalists and Viticulturalists", "1"),
    ("12", "Forestry and Natural Environment Workers", "1"),
    ("13", "Animal Husbandry and Breeding Workers", "1"),
    ("14", "Fishing and Aquaculture Workers", "1"),
    ("21", "Mining and Drilling Workers", "2"),
    ("22", "Stone, Concrete and Mineral Processing Workers", "2"),
    ("23", "Glass and Ceramics Workers", "2"),
    ("24", "Construction Materials and Building Products Workers", "2"),
    ("31", "Chemical, Pharmaceutical and Cosmetics Production Workers", "3"),
    ("32", "Plastics and Rubber Processing Workers", "3"),
    ("33", "Wood and Paper Production Workers", "3"),
    ("34", "Leather and Textile Manufacturing Workers", "3"),
    ("41", "Paper Processing and Printing Workers", "4"),
    ("42", "Wood, Furniture and Interior Construction Workers", "4"),
    ("43", "Food and Beverage Production Workers", "4"),
    ("44", "Clothing and Fashion Workers", "4"),
    ("51", "Metal Production and Metalworking Specialists", "5"),
    ("52", "Machine and Vehicle Technology Specialists", "5"),
    ("53", "Precision Engineering, Optics and Measurement Workers", "5"),
    ("54", "Industrial Engineering and Production Planning Professionals", "5"),
    ("55", "Aerospace Technology Workers", "5"),
    ("61", "Electrical Engineering and Technology Workers", "6"),
    ("62", "Electronics and Automation Technology Workers", "6"),
    ("63", "IT System Management and Development Specialists", "6"),
    ("64", "Telecommunications and Network Technology Workers", "6"),
    ("71", "Architecture, Urban Planning and Surveying Professionals", "7"),
    ("72", "Engineering and Technical Services Professionals", "7"),
    ("73", "Mathematics, Statistics and Scientific Computing Professionals", "7"),
    ("74", "Natural Sciences and Geoscience Professionals", "7"),
    ("75", "Medical Physics and Nuclear Technology Workers", "7"),
    ("81", "Building Construction Workers", "8"),
    ("82", "Structural and Civil Engineering Workers", "8"),
    ("83", "Interior Fitting and Insulation Workers", "8"),
    ("84", "Painting, Coating and Varnishing Workers", "8"),
    ("91", "Commerce and Retail Specialists", "9"),
    ("92", "Tourism, Hospitality and Personal Services Workers", "9"),
    ("93", "Healthcare, Body Care and Beauty Workers", "9"),
    ("94", "Media, Documentation and Information Services Workers", "9"),
    ("95", "Legal, Tax and Business Consulting Professionals", "9"),
    ("96", "Teaching and Training Professionals", "9"),
    ("97", "Social Work, Community and Religious Services Workers", "9"),
    ("98", "Security, Supervision and Surveillance Workers", "9"),
    ("01", "Military Service Occupations", "0"),
    ("02", "Civil Protection and Disaster Management Occupations", "0"),
]

# Broad crosswalk from KldB area to ISCO-08 major group
KLDB_TO_ISCO: dict[str, str] = {
    "1": "6",
    "2": "8",
    "3": "8",
    "4": "8",
    "5": "7",
    "6": "3",
    "7": "2",
    "8": "3",
    "9": "5",
    "0": "0",
}


async def ingest_kldb_2010(conn) -> int:
    """Ingest KldB 2010 into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "kldb_2010",
        "KldB 2010",
        "Klassifikation der Berufe 2010 (German Classification of Occupations)",
        "2010",
        "Germany",
        "Bundesagentur fuer Arbeit",
    )

    count = 0
    for seq, (code, title) in enumerate(AREAS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "kldb_2010", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(MAIN_GROUPS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "kldb_2010", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'kldb_2010'",
        count,
    )

    for kldb_code, isco_code in KLDB_TO_ISCO.items():
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
                "kldb_2010", kldb_code, "isco_2008", isco_code, "broad",
                "KldB 2010 occupational area to ISCO-08 major group",
            )

    return count
