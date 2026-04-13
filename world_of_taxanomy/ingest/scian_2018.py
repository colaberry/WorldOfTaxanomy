"""SCIAN 2018 (Mexico) ingester.

Sistema de Clasificacion Industrial de America del Norte (SCIAN), 2018 edition.
Published by INEGI (Instituto Nacional de Estadistica y Geografia), Mexico.
SCIAN is the Spanish-language adaptation of NAICS, co-developed by Mexico, Canada, and USA.
Mexico uses the same sector codes as NAICS 2017/2022 at the 2-digit level.

Structure:
  Sector (2-digit) = level 0  (20 sectors, same as NAICS)
  Subsector (3-digit) = level 1  -- not included in skeleton
  Industry Group (4-digit) = level 2  -- not included in skeleton
  National Industry (5-digit) = level 3  -- not included in skeleton

Source: https://www.inegi.org.mx/app/scian/
"""

from typing import Optional

# -- Sector structure (2-digit, mirrors NAICS) --------------------------------
# SCIAN sectors use the same 2-digit codes as NAICS.
# Spanish/Mexican context titles provided.

SCIAN_SECTORS = {
    "11": "Agriculture, Animal Husbandry, Forestry, Fishing and Hunting",
    "21": "Mining",
    "22": "Electric Power Generation, Transmission and Distribution; Water and Gas Piped to the Final Consumer",
    "23": "Construction",
    "31": "Manufacturing I (Food, Textiles, Apparel, Leather)",
    "32": "Manufacturing II (Paper, Printing, Petroleum, Chemicals, Plastics, Non-metallic Minerals)",
    "33": "Manufacturing III (Primary Metals, Fabricated Metal Products, Machinery, Electronics, Transport Equipment)",
    "43": "Wholesale Trade",
    "46": "Retail Trade",
    "48": "Air, Water, Rail and Ground Transportation of Passengers and Cargo I",
    "49": "Air, Water, Rail and Ground Transportation of Passengers and Cargo II; Postal and Courier Services",
    "51": "Mass Media Information",
    "52": "Financial Services and Insurance",
    "53": "Real Estate and Rental of Movable and Intangible Property",
    "54": "Professional, Scientific and Technical Services",
    "55": "Corporate and Enterprise Management",
    "56": "Business Support Services, Waste Management and Remediation",
    "61": "Educational Services",
    "62": "Health and Social Assistance",
    "71": "Recreational, Cultural and Sporting Services",
    "72": "Temporary Accommodation and Food and Beverage Preparation",
    "81": "Other Services Except Government Activities",
    "93": "Government, Legislative, Judicial, and International and Extraterritorial Activities",
}

# -- SCIAN -> NAICS 2022 sector-level mapping ---------------------------------
# SCIAN and NAICS share the same sector code structure.
# Most map exactly; subsector-level divergences exist but sections align.

SCIAN_TO_NAICS_MAPPING = {
    "11": [("11", "exact")],
    "21": [("21", "exact")],
    "22": [("22", "exact")],
    "23": [("23", "exact")],
    "31": [("31", "exact")],
    "32": [("32", "exact")],
    "33": [("33", "exact")],
    "43": [("42", "broad")],   # SCIAN 43 Wholesale -> NAICS 42 Wholesale
    "46": [("44", "broad"), ("45", "broad")],  # SCIAN 46 Retail -> NAICS 44-45
    "48": [("48", "exact")],
    "49": [("49", "exact")],
    "51": [("51", "exact")],
    "52": [("52", "exact")],
    "53": [("53", "exact")],
    "54": [("54", "exact")],
    "55": [("55", "exact")],
    "56": [("56", "exact")],
    "61": [("61", "exact")],
    "62": [("62", "exact")],
    "71": [("71", "exact")],
    "72": [("72", "exact")],
    "81": [("81", "exact")],
    "93": [("92", "broad")],   # SCIAN 93 Government -> NAICS 92 Public Admin
}

# -- Main ingestion ------------------------------------------------------------


async def ingest_scian_2018(conn) -> int:
    """Ingest SCIAN 2018 (Mexico) sector-level codes.

    Inserts 2-digit sector codes and creates NAICS 2022 equivalence edges.

    Args:
        conn: asyncpg connection

    Returns:
        Number of codes ingested.
    """
    await conn.execute("""
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ('scian_2018', 'SCIAN 2018',
                'Sistema de Clasificacion Industrial de America del Norte 2018',
                'Mexico', '2018',
                'INEGI - Instituto Nacional de Estadistica y Geografia',
                '#A855F7')
        ON CONFLICT (id) DO UPDATE SET node_count = 0
    """)

    count = 0
    seq = 0
    for code in sorted(SCIAN_SECTORS.keys()):
        title = SCIAN_SECTORS[code]
        seq += 1
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ('scian_2018', $1, $2, 0, NULL, $1, TRUE, $3)
            ON CONFLICT (system_id, code) DO NOTHING
        """, code, title, seq)
        count += 1

    edge_count = 0
    for scian_code, naics_targets in SCIAN_TO_NAICS_MAPPING.items():
        for naics_code, match_type in naics_targets:
            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('scian_2018', $1, 'naics_2022', $2, $3)
                ON CONFLICT DO NOTHING
            """, scian_code, naics_code, match_type)
            edge_count += 1

            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('naics_2022', $1, 'scian_2018', $2, $3)
                ON CONFLICT DO NOTHING
            """, naics_code, scian_code, match_type)
            edge_count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'scian_2018'",
        count,
    )

    print(f"  Ingested {count} SCIAN 2018 sector codes, {edge_count} equivalence edges")
    return count
