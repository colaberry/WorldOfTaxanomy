"""National Standard of the People's Republic of China - Industrial Classification for National Economic Activities (GB/T 4754-2017) ingester.

Source: National Bureau of Statistics of China (NBS). Hand-coded from public standard.
Aligned with ISIC Rev 4. Crosswalk edges to isic_rev4 created at section level.
"""
from typing import Optional, List, Tuple

SECTIONS = {
    "A": "Agriculture, Forestry, Animal Husbandry and Fishery",
    "B": "Mining and Quarrying",
    "C": "Manufacturing",
    "D": "Production and Supply of Electricity, Heat, Gas and Water",
    "E": "Construction",
    "F": "Wholesale and Retail Trade",
    "G": "Transportation, Storage and Post",
    "H": "Accommodation and Food Service Activities",
    "I": "Information Transmission, Software and Information Technology Services",
    "J": "Financial Intermediation",
    "K": "Real Estate Activities",
    "L": "Leasing and Business Services",
    "M": "Scientific Research and Technical Services",
    "N": "Water Conservancy, Environment and Public Facilities Management",
    "O": "Residential Services, Repair and Other Services",
    "P": "Education",
    "Q": "Health and Social Work",
    "R": "Culture, Sports and Entertainment",
    "S": "Public Administration, Social Security and Social Organization",
    "T": "International Organizations and Institutions",
}

# (section_code, division_code, division_title)
DIVISIONS: List[Tuple[str, str, str]] = [
    ("A", "01", "Crop cultivation"),
    ("A", "02", "Forestry"),
    ("A", "03", "Animal husbandry"),
    ("A", "04", "Fishery"),
    ("A", "05", "Agriculture, forestry, animal husbandry and fishery support activities"),
    ("B", "06", "Coal mining and washing"),
    ("B", "07", "Petroleum and natural gas extraction"),
    ("B", "08", "Ferrous metal mining and dressing"),
    ("B", "09", "Non-ferrous metal mining and dressing"),
    ("B", "10", "Non-metallic mineral mining and dressing"),
    ("B", "11", "Mining and processing of other ores"),
    ("B", "12", "Professional mining activities"),
    ("C", "13", "Agricultural and sideline food processing"),
    ("C", "14", "Food manufacturing"),
    ("C", "15", "Wine, beverages and refined tea manufacturing"),
    ("C", "16", "Tobacco product manufacturing"),
    ("C", "17", "Textile industry"),
    ("C", "18", "Textile apparel, clothing manufacturing"),
    ("C", "19", "Leather, fur, feather and footwear manufacturing"),
    ("C", "20", "Wood processing, wood, bamboo, rattan, palm and grass products"),
    ("C", "21", "Furniture manufacturing"),
    ("C", "22", "Paper and paper products industry"),
    ("C", "23", "Printing and recording media reproduction"),
    ("C", "24", "Culture, education, sports and entertainment products"),
    ("C", "25", "Petroleum, coal and other fuel processing"),
    ("C", "26", "Chemical raw materials and chemical products manufacturing"),
    ("C", "27", "Pharmaceutical manufacturing"),
    ("C", "28", "Chemical fiber manufacturing"),
    ("C", "29", "Rubber and plastics products industry"),
    ("C", "30", "Non-metallic mineral products industry"),
    ("C", "31", "Ferrous metal smelting and rolling processing"),
    ("C", "32", "Non-ferrous metal smelting and rolling processing"),
    ("C", "33", "Metal products industry"),
    ("C", "34", "General equipment manufacturing"),
    ("C", "35", "Special equipment manufacturing"),
    ("C", "36", "Automobile manufacturing"),
    ("C", "37", "Railway, shipbuilding, aerospace and other transportation equipment"),
    ("C", "38", "Electrical machinery and equipment manufacturing"),
    ("C", "39", "Computer, communication and other electronic equipment"),
    ("C", "40", "Measuring instrument and machinery manufacturing"),
    ("C", "41", "Other manufacturing"),
    ("C", "42", "Comprehensive utilization of waste resources"),
    ("C", "43", "Metal products, machinery and equipment repair"),
    ("D", "44", "Electric power, heat production and supply"),
    ("D", "45", "Gas production and supply"),
    ("D", "46", "Water production and supply"),
    ("E", "47", "Housing construction"),
    ("E", "48", "Civil engineering construction"),
    ("E", "49", "Construction installation"),
    ("E", "50", "Building decoration, decoration and other construction"),
    ("F", "51", "Wholesale"),
    ("F", "52", "Retail"),
    ("G", "53", "Railway transportation"),
    ("G", "54", "Road transportation"),
    ("G", "55", "Water transportation"),
    ("G", "56", "Air transportation"),
    ("G", "57", "Pipeline transportation"),
    ("G", "58", "Multimodal and transport agency"),
    ("G", "59", "Loading and unloading and handling and storage"),
    ("G", "60", "Postal"),
    ("H", "61", "Accommodation"),
    ("H", "62", "Food and beverage service"),
    ("I", "63", "Telecom, broadcasting, satellite transmission services"),
    ("I", "64", "Internet and related services"),
    ("I", "65", "Software and information technology service industry"),
    ("J", "66", "Monetary and financial services"),
    ("J", "67", "Capital market services"),
    ("J", "68", "Insurance"),
    ("J", "69", "Other financial industry"),
    ("K", "70", "Real estate"),
    ("L", "71", "Leasing"),
    ("L", "72", "Business services"),
    ("M", "73", "Research and experimental development"),
    ("M", "74", "Professional technical services"),
    ("M", "75", "Science and technology promotion and application services"),
    ("N", "76", "Water conservancy management"),
    ("N", "77", "Ecological protection and environmental governance"),
    ("N", "78", "Public facilities management"),
    ("N", "79", "Land management"),
    ("O", "80", "Residential services"),
    ("O", "81", "Automobile, motorcycle, electronic appliance repair and maintenance"),
    ("O", "82", "Other repair services"),
    ("O", "83", "Other services"),
    ("P", "84", "Education"),
    ("Q", "85", "Health"),
    ("Q", "86", "Social work"),
    ("R", "87", "News and publishing"),
    ("R", "88", "Radio, television, film and recording production"),
    ("R", "89", "Culture and art"),
    ("R", "90", "Sports"),
    ("R", "91", "Entertainment"),
    ("S", "92", "Chinese Communist Party organs"),
    ("S", "93", "National institutions"),
    ("S", "94", "People's Political Consultative Conference, democratic parties"),
    ("S", "95", "Social security"),
    ("S", "96", "Mass organizations, social organizations and other members"),
    ("S", "97", "International organizations"),
    ("T", "99", "International organizations and institutions stationed in China"),
]

SECTION_TO_ISIC = {
    "A": "A",
    "B": "B",
    "C": "C",
    "D": "D",
    "E": "F",
    "F": "G",
    "G": "H",
    "H": "I",
    "I": "J",
    "J": "K",
    "K": "L",
    "L": "M",
    "M": "M",
    "N": "E",
    "O": "S",
    "P": "P",
    "Q": "Q",
    "R": "R",
    "S": "O",
    "T": "U",
}

async def ingest_gbt_4754(conn) -> int:
    """Ingest GB/T 4754-2017 (China) into classification_system and classification_node.
    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, region, version, authority, tint_color)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "gbt_4754", "GB/T 4754-2017", "National Standard of the People's Republic of China - Industrial Classification for National Economic Activities (GB/T 4754-2017)",
        "China", "2017", "National Bureau of Statistics of China (NBS)", "#EF4444",
    )

    count = 0
    for seq, (code, title) in enumerate(SECTIONS.items(), 1):
        # section has children (divisions) => not a leaf
        is_leaf = False
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, 0, NULL, $2, $4, $5)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "gbt_4754", code, title, is_leaf, seq,
        )
        count += 1

    seq = 0
    for section_code, div_code, div_title in DIVISIONS:
        seq += 1
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, 1, $4, $5, TRUE, $6)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "gbt_4754", div_code, div_title, section_code, section_code, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'gbt_4754'",
        count,
    )

    # Equivalence edges to ISIC Rev 4 (section level)
    edge_count = 0
    for sec_code, isic_code in SECTION_TO_ISIC.items():
        await conn.execute(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ($1, $2, 'isic_rev4', $3, 'broad')
               ON CONFLICT DO NOTHING""",
            "gbt_4754", sec_code, isic_code,
        )
        await conn.execute(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ('isic_rev4', $1, $2, $3, 'broad')
               ON CONFLICT DO NOTHING""",
            isic_code, "gbt_4754", sec_code,
        )
        edge_count += 1

    print(f"  {count} GB/T 4754-2017 nodes, {edge_count * 2} equivalence edges")
    return count
