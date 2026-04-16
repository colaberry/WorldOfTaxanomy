"""CNAE 2.0 (Brazil) ingester.

Classificacao Nacional de Atividades Economicas (CNAE), version 2.0 (2007).
Published by IBGE (Instituto Brasileiro de Geografia e Estatistica).
Aligned to ISIC Rev 4 - Brazil co-developed CNAE 2.0 to mirror ISIC structure.

Structure:
  Section (letter A-U) = level 0  (21 codes)
  Division (2-digit)   = level 1  -- not included in skeleton
  Group (3-digit)      = level 2  -- not included in skeleton
  Class (4-digit)      = level 3  -- not included in skeleton
  Subclass (7-digit)   = level 4  -- not included in skeleton

Source: https://cnae.ibge.gov.br/
"""

from typing import Optional

# -- Section structure (A-U) --------------------------------------------------

CNAE_SECTIONS = {
    "A": "Agriculture, Livestock, Forestry, Fishing and Aquaculture",
    "B": "Mining and Quarrying",
    "C": "Manufacturing",
    "D": "Electricity and Gas",
    "E": "Water, Sewerage, Waste Management and Remediation Activities",
    "F": "Construction",
    "G": "Wholesale and Retail Trade; Repair of Motor Vehicles and Motorcycles",
    "H": "Transportation, Storage and Mail",
    "I": "Accommodation and Food Service Activities",
    "J": "Information and Communication",
    "K": "Financial, Insurance and Related Services Activities",
    "L": "Real Estate Activities",
    "M": "Professional, Scientific and Technical Activities",
    "N": "Administrative and Complementary Service Activities",
    "O": "Public Administration, Defence and Social Security",
    "P": "Education",
    "Q": "Human Health and Social Work Activities",
    "R": "Arts, Culture, Sport and Recreation Activities",
    "S": "Other Service Activities",
    "T": "Services of Households as Employers; Undifferentiated Goods and Services",
    "U": "International Organisations and Other Extra-Territorial Bodies",
}

# -- CNAE -> ISIC Rev 4 section-level mapping ---------------------------------
# CNAE 2.0 is structurally aligned to ISIC Rev 4 at section level.
# Match types are 'exact' where the sections fully correspond, 'broad' otherwise.

CNAE_TO_ISIC_MAPPING = {
    "A": [("A", "exact")],
    "B": [("B", "exact")],
    "C": [("C", "exact")],
    "D": [("D", "exact")],
    "E": [("E", "exact")],
    "F": [("F", "exact")],
    "G": [("G", "exact")],
    "H": [("H", "exact")],
    "I": [("I", "exact")],
    "J": [("J", "exact")],
    "K": [("K", "exact")],
    "L": [("L", "exact")],
    "M": [("M", "exact")],
    "N": [("N", "exact")],
    "O": [("O", "exact")],
    "P": [("P", "exact")],
    "Q": [("Q", "exact")],
    "R": [("R", "exact")],
    "S": [("S", "exact")],
    "T": [("T", "exact")],
    "U": [("U", "exact")],
}

# -- Main ingestion ------------------------------------------------------------


async def ingest_cnae_2012(conn) -> int:
    """Ingest CNAE 2.0 (Brazil) section-level codes.

    Inserts the 21 section codes (A-U) with English titles and creates
    ISIC Rev 4 equivalence edges at section level.

    Args:
        conn: asyncpg connection

    Returns:
        Number of codes ingested.
    """
    await conn.execute("""
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ('cnae_2012', 'CNAE 2.0',
                'Classificacao Nacional de Atividades Economicas 2.0',
                'Brazil', '2.0 (2007)',
                'IBGE - Instituto Brasileiro de Geografia e Estatistica',
                '#22C55E')
        ON CONFLICT (id) DO UPDATE SET node_count = 0
    """)

    count = 0
    seq = 0
    for code in sorted(CNAE_SECTIONS.keys()):
        title = CNAE_SECTIONS[code]
        seq += 1
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ('cnae_2012', $1, $2, 0, NULL, $1, TRUE, $3)
            ON CONFLICT (system_id, code) DO NOTHING
        """, code, title, seq)
        count += 1

    edge_count = 0
    for cnae_code, isic_targets in CNAE_TO_ISIC_MAPPING.items():
        for isic_code, match_type in isic_targets:
            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('cnae_2012', $1, 'isic_rev4', $2, $3)
                ON CONFLICT DO NOTHING
            """, cnae_code, isic_code, match_type)
            edge_count += 1

            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('isic_rev4', $1, 'cnae_2012', $2, $3)
                ON CONFLICT DO NOTHING
            """, isic_code, cnae_code, match_type)
            edge_count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'cnae_2012'",
        count,
    )

    print(f"  Ingested {count} CNAE 2.0 section codes, {edge_count} equivalence edges")
    return count
