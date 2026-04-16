"""Korean Standard Industry Classification (10th revision, 2017) ingester.

Source: Statistics Korea (Kostat). Hand-coded from public standard.
Aligned with ISIC Rev 4. Crosswalk edges to isic_rev4 created at section level.
"""
from typing import Optional, List, Tuple

SECTIONS = {
    "A": "Agriculture, Forestry and Fishing",
    "B": "Mining and Quarrying",
    "C": "Manufacturing",
    "D": "Electricity, Gas, Steam and Air Conditioning Supply",
    "E": "Water Supply, Sewerage, Waste Management and Remediation",
    "F": "Construction",
    "G": "Wholesale and Retail Trade",
    "H": "Transportation and Storage",
    "I": "Accommodation and Food Service Activities",
    "J": "Information and Communication",
    "K": "Financial and Insurance Activities",
    "L": "Real Estate Activities",
    "M": "Professional, Scientific and Technical Activities",
    "N": "Business Facilities Management and Support Service Activities",
    "O": "Public Administration and Defence",
    "P": "Education",
    "Q": "Human Health and Social Work Activities",
    "R": "Arts, Sports and Recreation Activities",
    "S": "Membership Organizations, Repair and Other Personal Service Activities",
    "T": "Activities of Households as Employers",
    "U": "Activities of Extraterritorial Organizations and Bodies",
}

# (section_code, division_code, division_title)
DIVISIONS: List[Tuple[str, str, str]] = [
    ("A", "01", "Crop and animal production, hunting and related service activities"),
    ("A", "02", "Forestry and logging"),
    ("A", "03", "Fishing and aquaculture"),
    ("B", "05", "Mining of coal and lignite"),
    ("B", "06", "Extraction of crude petroleum and natural gas"),
    ("B", "07", "Mining of metal ores"),
    ("B", "08", "Other mining and quarrying"),
    ("B", "09", "Mining support service activities"),
    ("C", "10", "Manufacture of food products"),
    ("C", "11", "Manufacture of beverages"),
    ("C", "12", "Manufacture of tobacco products"),
    ("C", "13", "Manufacture of textiles"),
    ("C", "14", "Manufacture of wearing apparel"),
    ("C", "15", "Manufacture of leather and related products"),
    ("C", "16", "Manufacture of wood and products of wood"),
    ("C", "17", "Manufacture of paper and paper products"),
    ("C", "18", "Printing and reproduction of recorded media"),
    ("C", "19", "Manufacture of coke and refined petroleum products"),
    ("C", "20", "Manufacture of chemicals and chemical products"),
    ("C", "21", "Manufacture of pharmaceuticals"),
    ("C", "22", "Manufacture of rubber and plastics products"),
    ("C", "23", "Manufacture of other non-metallic mineral products"),
    ("C", "24", "Manufacture of basic metals"),
    ("C", "25", "Manufacture of fabricated metal products"),
    ("C", "26", "Manufacture of electronic components and computers"),
    ("C", "27", "Manufacture of electrical equipment"),
    ("C", "28", "Manufacture of machinery and equipment NEC"),
    ("C", "29", "Manufacture of motor vehicles and trailers"),
    ("C", "30", "Manufacture of other transport equipment"),
    ("C", "31", "Manufacture of furniture"),
    ("C", "32", "Other manufacturing"),
    ("C", "33", "Repair and installation of machinery and equipment"),
    ("D", "35", "Electricity, gas, steam and air conditioning supply"),
    ("E", "36", "Water collection, treatment and supply"),
    ("E", "37", "Sewerage"),
    ("E", "38", "Waste collection, treatment and disposal"),
    ("E", "39", "Remediation activities"),
    ("F", "41", "Construction of buildings"),
    ("F", "42", "Civil engineering"),
    ("F", "43", "Specialised construction activities"),
    ("G", "45", "Wholesale and retail trade of motor vehicles"),
    ("G", "46", "Wholesale trade except motor vehicles"),
    ("G", "47", "Retail trade except motor vehicles"),
    ("H", "49", "Land transport and transport via pipelines"),
    ("H", "50", "Water transport"),
    ("H", "51", "Air transport"),
    ("H", "52", "Warehousing and support activities for transportation"),
    ("H", "53", "Postal and courier activities"),
    ("I", "55", "Accommodation"),
    ("I", "56", "Food and beverage service activities"),
    ("J", "58", "Publishing activities"),
    ("J", "59", "Motion picture, video and television programme production"),
    ("J", "60", "Programming and broadcasting activities"),
    ("J", "61", "Telecommunications"),
    ("J", "62", "Computer programming, consultancy and related activities"),
    ("J", "63", "Information service activities"),
    ("K", "64", "Financial service activities"),
    ("K", "65", "Insurance, reinsurance and pension funding"),
    ("K", "66", "Activities auxiliary to financial services"),
    ("L", "68", "Real estate activities"),
    ("M", "70", "Activities of head offices; management consultancy"),
    ("M", "71", "Architectural and engineering activities"),
    ("M", "72", "Scientific research and development"),
    ("M", "73", "Advertising and market research"),
    ("M", "74", "Other professional, scientific and technical activities"),
    ("M", "75", "Veterinary activities"),
    ("N", "77", "Rental and leasing activities"),
    ("N", "78", "Employment activities"),
    ("N", "79", "Travel agency and tour operator activities"),
    ("N", "80", "Security and investigation activities"),
    ("N", "81", "Services to buildings and landscape activities"),
    ("N", "82", "Office administrative and business support activities"),
    ("O", "84", "Public administration and defence; compulsory social security"),
    ("P", "85", "Education"),
    ("Q", "86", "Human health activities"),
    ("Q", "87", "Residential care activities"),
    ("Q", "88", "Social work activities without accommodation"),
    ("R", "90", "Creative, arts and entertainment activities"),
    ("R", "91", "Libraries, archives, museums and other cultural activities"),
    ("R", "92", "Gambling and betting activities"),
    ("R", "93", "Sports activities and amusement and recreation activities"),
    ("S", "94", "Activities of membership organisations"),
    ("S", "95", "Repair of computers and personal and household goods"),
    ("S", "96", "Other personal service activities"),
    ("T", "97", "Activities of households as employers of domestic personnel"),
    ("T", "98", "Undifferentiated goods and services producing activities"),
    ("U", "99", "Activities of extraterritorial organisations and bodies"),
]

SECTION_TO_ISIC = {
    "A": "A",
    "B": "B",
    "C": "C",
    "D": "D",
    "E": "E",
    "F": "F",
    "G": "G",
    "H": "H",
    "I": "I",
    "J": "J",
    "K": "K",
    "L": "L",
    "M": "M",
    "N": "N",
    "O": "O",
    "P": "P",
    "Q": "Q",
    "R": "R",
    "S": "S",
    "T": "T",
    "U": "U",
}

async def ingest_ksic_2017(conn) -> int:
    """Ingest KSIC 2017 (South Korea) into classification_system and classification_node.
    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, region, version, authority, tint_color)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "ksic_2017", "KSIC 2017", "Korean Standard Industry Classification (10th revision, 2017)",
        "South Korea", "10th revision (2017)", "Statistics Korea (Kostat)", "#3B82F6",
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
            "ksic_2017", code, title, is_leaf, seq,
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
            "ksic_2017", div_code, div_title, section_code, section_code, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'ksic_2017'",
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
            "ksic_2017", sec_code, isic_code,
        )
        await conn.execute(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ('isic_rev4', $1, $2, $3, 'broad')
               ON CONFLICT DO NOTHING""",
            isic_code, "ksic_2017", sec_code,
        )
        edge_count += 1

    print(f"  {count} KSIC 2017 nodes, {edge_count * 2} equivalence edges")
    return count
